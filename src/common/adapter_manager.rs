/// Implements common functionality for adapters across platforms.
// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.
//
// Some portions of this file are taken and/or modified from Rumble
// (https://github.com/mwylde/rumble), using a dual MIT/Apache License under the
// following copyright:
//
// Copyright (c) 2014 The Rust Project Developers
use crate::api::{CentralEvent, Peripheral};
use crate::platform::PeripheralId;
use dashmap::{
    DashMap,
    mapref::{entry::Entry, one::RefMut},
};
use futures::stream::{Stream, StreamExt};
use log::trace;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// Matches BlueZ's default `TemporaryTimeout` for discovered devices.
const PERIPHERAL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct AdapterManager<PeripheralType>
where
    PeripheralType: Peripheral,
{
    peripherals: DashMap<PeripheralId, PeripheralType>,
    last_seen: DashMap<PeripheralId, Instant>,
    events_channel: broadcast::Sender<CentralEvent>,
}

impl<PeripheralType: Peripheral + 'static> Default for AdapterManager<PeripheralType> {
    fn default() -> Self {
        let (broadcast_sender, _) = broadcast::channel(16);
        AdapterManager {
            peripherals: DashMap::new(),
            last_seen: DashMap::new(),
            events_channel: broadcast_sender,
        }
    }
}

impl<PeripheralType> AdapterManager<PeripheralType>
where
    PeripheralType: Peripheral + 'static,
{
    pub fn emit(&self, event: CentralEvent) {
        match &event {
            CentralEvent::DeviceDisconnected(id) => {
                self.peripherals.remove(id);
                self.last_seen.remove(id);
            }
            CentralEvent::DeviceDiscovered(id)
            | CentralEvent::DeviceUpdated(id)
            | CentralEvent::ManufacturerDataAdvertisement { id, .. }
            | CentralEvent::ServiceDataAdvertisement { id, .. }
            | CentralEvent::ServicesAdvertisement { id, .. } => self.mark_seen(id),
            _ => {}
        }

        if let Err(lost) = self.events_channel.send(event) {
            trace!("Lost central event, while nothing subscribed: {:?}", lost);
        }
    }

    pub fn event_stream(&self) -> Pin<Box<dyn Stream<Item = CentralEvent> + Send>> {
        let receiver = self.events_channel.subscribe();
        Box::pin(BroadcastStream::new(receiver).filter_map(|x| async move { x.ok() }))
    }

    /// Inserts a peripheral if absent and returns the retained instance,
    /// preserving any existing connection state and characteristics.
    pub fn add_peripheral(&self, peripheral: PeripheralType) -> PeripheralType {
        let id = peripheral.id();
        match self.peripherals.entry(id.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                self.last_seen.insert(id, Instant::now());
                entry.insert(peripheral).value().clone()
            }
        }
    }

    /// Like [`add_peripheral`](Self::add_peripheral), but for peripherals requested explicitly,
    /// which are exempt from stale pruning.
    pub fn add_known_peripheral(&self, peripheral: PeripheralType) -> PeripheralType {
        let peripheral = self.add_peripheral(peripheral);
        self.last_seen.remove(&peripheral.id());
        peripheral
    }

    /// Removes cached peripherals not retained by the backend predicate.
    ///
    /// The backend is responsible for retaining connected and pending peripherals.
    /// The predicate runs under the map lock and must not re-enter this manager.
    pub fn clear_peripherals(&self, mut should_retain: impl FnMut(&PeripheralType) -> bool) {
        self.peripherals
            .retain(|_, peripheral| should_retain(peripheral));
        self.last_seen.clear();
    }

    pub fn mark_seen(&self, id: &PeripheralId) {
        if let Some(mut seen) = self.last_seen.get_mut(id) {
            *seen = Instant::now();
        }
    }

    /// Removes disconnected peripherals not seen within [`PERIPHERAL_TIMEOUT`] and returns their ids.
    pub async fn prune_stale_peripherals(&self) -> Vec<PeripheralId> {
        let stale: Vec<PeripheralId> = self
            .last_seen
            .iter()
            .filter(|entry| entry.value().elapsed() >= PERIPHERAL_TIMEOUT)
            .map(|entry| entry.key().clone())
            .collect();

        let mut pruned = Vec::new();
        for id in stale {
            let Some(peripheral) = self.peripheral(&id) else {
                continue;
            };
            // Connected peripherals stop advertising, so they must not be pruned.
            if peripheral.is_connected().await.unwrap_or(false) {
                self.mark_seen(&id);
            } else if self
                .last_seen
                .remove_if(&id, |_, seen| seen.elapsed() >= PERIPHERAL_TIMEOUT)
                .is_some()
            {
                self.peripherals.remove(&id);
                pruned.push(id);
            }
        }

        pruned
    }

    pub fn peripherals(&self) -> Vec<PeripheralType> {
        self.peripherals
            .iter()
            .map(|val| val.value().clone())
            .collect()
    }

    // Only used on windows and macOS/iOS, so turn off deadcode so we don't get warnings on android/linux.
    #[allow(dead_code)]
    pub fn peripheral_mut(
        &self,
        id: &PeripheralId,
    ) -> Option<RefMut<'_, PeripheralId, PeripheralType>> {
        self.peripherals.get_mut(id)
    }

    pub fn peripheral(&self, id: &PeripheralId) -> Option<PeripheralType> {
        self.peripherals.get(id).map(|val| val.value().clone())
    }
}

#[cfg(all(test, any(target_vendor = "apple", target_os = "windows")))]
mod tests {
    use super::*;
    use crate::Result;
    use crate::api::{
        BDAddr, Characteristic, Descriptor, PeripheralProperties, Service, ValueNotification,
        WriteType,
    };
    use async_trait::async_trait;
    use std::collections::BTreeSet;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone, Debug)]
    struct TestPeripheral {
        id: PeripheralId,
        state: Arc<AtomicUsize>,
    }

    impl TestPeripheral {
        fn new() -> Self {
            #[cfg(target_vendor = "apple")]
            let id = uuid::Uuid::nil().into();
            #[cfg(target_os = "windows")]
            let id = BDAddr::default().into();
            Self {
                id,
                state: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl Peripheral for TestPeripheral {
        fn id(&self) -> PeripheralId {
            self.id.clone()
        }

        fn address(&self) -> BDAddr {
            unreachable!()
        }

        fn mtu(&self) -> u16 {
            unreachable!()
        }

        fn services(&self) -> BTreeSet<Service> {
            unreachable!()
        }

        async fn properties(&self) -> Result<Option<PeripheralProperties>> {
            unreachable!()
        }

        async fn is_connected(&self) -> Result<bool> {
            unreachable!()
        }

        async fn connect(&self) -> Result<()> {
            unreachable!()
        }

        async fn disconnect(&self) -> Result<()> {
            unreachable!()
        }

        async fn discover_services(&self) -> Result<()> {
            unreachable!()
        }

        async fn write(&self, _: &Characteristic, _: &[u8], _: WriteType) -> Result<()> {
            unreachable!()
        }

        async fn read(&self, _: &Characteristic) -> Result<Vec<u8>> {
            unreachable!()
        }

        async fn subscribe(&self, _: &Characteristic) -> Result<()> {
            unreachable!()
        }

        async fn unsubscribe(&self, _: &Characteristic) -> Result<()> {
            unreachable!()
        }

        async fn notifications(
            &self,
        ) -> Result<Pin<Box<dyn Stream<Item = ValueNotification> + Send>>> {
            unreachable!()
        }

        async fn write_descriptor(&self, _: &Descriptor, _: &[u8]) -> Result<()> {
            unreachable!()
        }

        async fn read_descriptor(&self, _: &Descriptor) -> Result<Vec<u8>> {
            unreachable!()
        }
    }

    #[test]
    fn duplicate_insertion_returns_existing_shared_state() {
        let manager = AdapterManager::default();
        let first = TestPeripheral::new();
        first.state.store(41, Ordering::SeqCst);
        let inserted = manager.add_peripheral(first.clone());
        assert!(Arc::ptr_eq(&first.state, &inserted.state));

        let duplicate = TestPeripheral::new();
        assert!(!Arc::ptr_eq(&first.state, &duplicate.state));
        let returned = manager.add_peripheral(duplicate.clone());
        assert!(Arc::ptr_eq(&first.state, &returned.state));
        assert_eq!(returned.state.fetch_add(1, Ordering::SeqCst), 41);
        assert_eq!(duplicate.state.load(Ordering::SeqCst), 0);

        let stored = manager.peripheral(&first.id()).unwrap();
        assert!(Arc::ptr_eq(&returned.state, &stored.state));
        assert_eq!(stored.state.load(Ordering::SeqCst), 42);
        assert_eq!(manager.peripherals().len(), 1);
    }

    #[test]
    fn concurrent_insertions_return_the_same_shared_state() {
        const WORKERS: usize = 16;
        let manager = AdapterManager::default();
        let barrier = Barrier::new(WORKERS);
        let returned = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..WORKERS)
                .map(|_| {
                    let manager = &manager;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let candidate = TestPeripheral::new();
                        barrier.wait();
                        let canonical = manager.add_peripheral(candidate);
                        canonical.state.fetch_add(1, Ordering::SeqCst);
                        canonical
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        let stored = manager.peripheral(&returned[0].id()).unwrap();
        assert_eq!(manager.peripherals().len(), 1);
        for peripheral in returned {
            assert!(Arc::ptr_eq(&stored.state, &peripheral.state));
            assert_eq!(peripheral.state.load(Ordering::SeqCst), WORKERS);
        }
    }
}

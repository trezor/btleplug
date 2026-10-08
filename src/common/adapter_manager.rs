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
use dashmap::{DashMap, mapref::entry::Entry};
use futures::stream::{Stream, StreamExt};
use log::{debug, trace};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// Matches BlueZ's default `TemporaryTimeout` for discovered devices.
const PERIPHERAL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct TrackedPeripheral<PeripheralType> {
    peripheral: PeripheralType,
    /// `None` exempts the peripheral from stale pruning.
    last_seen: Option<Instant>,
    connection_operations: usize,
}

impl<PeripheralType> TrackedPeripheral<PeripheralType> {
    fn is_stale(&self) -> bool {
        self.last_seen
            .is_some_and(|seen| seen.elapsed() >= PERIPHERAL_TIMEOUT)
    }
}

#[derive(Debug)]
pub struct AdapterManager<PeripheralType>
where
    PeripheralType: Peripheral,
{
    peripherals: DashMap<PeripheralId, TrackedPeripheral<PeripheralType>>,
    events_channel: broadcast::Sender<CentralEvent>,
}

#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(crate) struct ConnectionOperationGuard<PeripheralType: Peripheral> {
    manager: Arc<AdapterManager<PeripheralType>>,
    id: PeripheralId,
}

impl<PeripheralType: Peripheral> Drop for ConnectionOperationGuard<PeripheralType> {
    fn drop(&mut self) {
        if let Some(mut tracked) = self.manager.peripherals.get_mut(&self.id) {
            tracked.connection_operations = tracked.connection_operations.saturating_sub(1);
        }
    }
}

impl<PeripheralType: Peripheral + 'static> Default for AdapterManager<PeripheralType> {
    fn default() -> Self {
        let (broadcast_sender, _) = broadcast::channel(16);
        AdapterManager {
            peripherals: DashMap::new(),
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
            }
            CentralEvent::DeviceConnected(id) => {
                if let Some(mut tracked) = self.peripherals.get_mut(id) {
                    tracked.last_seen = None;
                }
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
        match self.peripherals.entry(peripheral.id()) {
            Entry::Occupied(entry) => entry.get().peripheral.clone(),
            Entry::Vacant(entry) => entry
                .insert(TrackedPeripheral {
                    peripheral,
                    last_seen: Some(Instant::now()),
                    connection_operations: 0,
                })
                .peripheral
                .clone(),
        }
    }

    /// Like [`add_peripheral`](Self::add_peripheral), but for peripherals requested explicitly,
    /// which are exempt from stale pruning.
    #[cfg_attr(target_os = "android", allow(dead_code))]
    pub fn add_known_peripheral(&self, peripheral: PeripheralType) -> PeripheralType {
        let mut tracked =
            self.peripherals
                .entry(peripheral.id())
                .or_insert_with(|| TrackedPeripheral {
                    peripheral,
                    last_seen: None,
                    connection_operations: 0,
                });
        tracked.last_seen = None;
        tracked.peripheral.clone()
    }

    pub fn clear_peripherals(&self) {
        self.peripherals.clear();
    }

    /// Removes a peripheral without emitting an event.
    #[cfg_attr(not(target_vendor = "apple"), allow(dead_code))]
    pub fn remove_peripheral(&self, id: &PeripheralId) {
        self.peripherals.remove(id);
    }

    pub fn mark_seen(&self, id: &PeripheralId) {
        if let Some(mut tracked) = self.peripherals.get_mut(id)
            && let Some(seen) = &mut tracked.last_seen
        {
            *seen = Instant::now();
        }
    }

    // The count lives on the entry; it is lost if the entry is removed and re-added mid-operation.
    #[cfg_attr(not(target_os = "windows"), allow(dead_code))]
    pub(crate) fn track_connection_operation(
        self: &Arc<Self>,
        id: PeripheralId,
    ) -> ConnectionOperationGuard<PeripheralType> {
        if let Some(mut tracked) = self.peripherals.get_mut(&id) {
            tracked.connection_operations += 1;
        }
        ConnectionOperationGuard {
            manager: self.clone(),
            id,
        }
    }

    /// Returns peripherals not seen within [`PERIPHERAL_TIMEOUT`] and not mid-connect/disconnect,
    /// regardless of their connection state.
    pub fn expired_peripherals(&self) -> Vec<PeripheralId> {
        self.peripherals
            .iter()
            .filter(|entry| entry.is_stale() && entry.connection_operations == 0)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Removes disconnected expired peripherals and returns their ids.
    #[cfg_attr(not(target_os = "windows"), allow(dead_code))]
    pub async fn prune_stale_peripherals(&self) -> Vec<PeripheralId> {
        let mut pruned = Vec::new();
        for id in self.expired_peripherals() {
            let Some(peripheral) = self.peripheral(&id) else {
                continue;
            };
            // Connected peripherals stop advertising, so they must not be pruned.
            match peripheral.is_connected().await {
                Ok(false) => {
                    if self
                        .peripherals
                        .remove_if(&id, |_, tracked| {
                            tracked.is_stale() && tracked.connection_operations == 0
                        })
                        .is_some()
                    {
                        pruned.push(id);
                    }
                }
                Ok(true) => self.mark_seen(&id),
                Err(error) => {
                    debug!("Keeping {id:?}, connection state unknown: {error}");
                    self.mark_seen(&id);
                }
            }
        }
        pruned
    }

    pub fn peripherals(&self) -> Vec<PeripheralType> {
        self.peripherals
            .iter()
            .map(|val| val.peripheral.clone())
            .collect()
    }

    pub fn peripheral(&self, id: &PeripheralId) -> Option<PeripheralType> {
        self.peripherals.get(id).map(|val| val.peripheral.clone())
    }
}

#[cfg(all(test, any(target_vendor = "apple", target_os = "windows")))]
mod tests {
    use super::*;
    use crate::api::{
        BDAddr, Characteristic, Descriptor, PeripheralProperties, Service, ValueNotification,
        WriteType,
    };
    use crate::{Error, Result};
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
        // `None` makes `is_connected` fail.
        connected: Option<bool>,
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
                connected: Some(false),
            }
        }
    }

    fn expire(manager: &AdapterManager<TestPeripheral>, id: &PeripheralId) {
        manager.peripherals.get_mut(id).unwrap().last_seen =
            Some(Instant::now() - PERIPHERAL_TIMEOUT);
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
            self.connected
                .ok_or_else(|| Error::RuntimeError("unknown connection state".to_string()))
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

    #[tokio::test]
    async fn pruning_keeps_peripheral_during_connection_operation() {
        let manager = Arc::new(AdapterManager::default());
        let peripheral = TestPeripheral::new();
        let id = peripheral.id();
        manager.add_peripheral(peripheral);
        expire(&manager, &id);
        let operation = manager.track_connection_operation(id.clone());

        assert!(manager.prune_stale_peripherals().await.is_empty());
        assert!(manager.peripheral(&id).is_some());

        drop(operation);
        assert_eq!(
            manager.peripherals.get(&id).unwrap().connection_operations,
            0
        );
    }

    #[tokio::test]
    async fn pruning_removes_stale_disconnected_peripheral() {
        let manager = AdapterManager::default();
        let id = manager.add_peripheral(TestPeripheral::new()).id();
        expire(&manager, &id);

        assert_eq!(manager.prune_stale_peripherals().await, vec![id.clone()]);
        assert!(manager.peripheral(&id).is_none());
        assert!(!manager.peripherals.contains_key(&id));
    }

    #[tokio::test]
    async fn pruning_keeps_known_peripheral() {
        let manager = AdapterManager::default();
        let id = manager.add_peripheral(TestPeripheral::new()).id();
        expire(&manager, &id);
        manager.add_known_peripheral(TestPeripheral::new());

        assert!(manager.prune_stale_peripherals().await.is_empty());
        assert!(manager.peripheral(&id).is_some());
    }

    #[test]
    fn mark_seen_keeps_known_peripheral_exempt() {
        let manager = AdapterManager::default();
        let id = manager.add_known_peripheral(TestPeripheral::new()).id();
        manager.mark_seen(&id);

        assert!(manager.peripherals.get(&id).unwrap().last_seen.is_none());
    }

    #[tokio::test]
    async fn mark_seen_resets_stale_timer() {
        let manager = AdapterManager::default();
        let id = manager.add_peripheral(TestPeripheral::new()).id();
        expire(&manager, &id);
        manager.mark_seen(&id);

        assert!(manager.prune_stale_peripherals().await.is_empty());
        assert!(manager.peripheral(&id).is_some());
    }

    #[tokio::test]
    async fn pruning_keeps_peripheral_with_unknown_connection_state() {
        let manager = AdapterManager::default();
        let mut peripheral = TestPeripheral::new();
        peripheral.connected = None;
        let id = manager.add_peripheral(peripheral).id();
        expire(&manager, &id);

        assert!(manager.prune_stale_peripherals().await.is_empty());
        assert!(manager.peripheral(&id).is_some());
        assert!(!manager.peripherals.get(&id).unwrap().is_stale());
    }

    #[tokio::test]
    async fn pruning_keeps_connected_peripheral() {
        let manager = AdapterManager::default();
        let mut peripheral = TestPeripheral::new();
        peripheral.connected = Some(true);
        let id = manager.add_peripheral(peripheral).id();
        expire(&manager, &id);

        assert!(manager.prune_stale_peripherals().await.is_empty());
        assert!(manager.peripheral(&id).is_some());
        assert!(!manager.peripherals.get(&id).unwrap().is_stale());
    }

    #[tokio::test]
    async fn device_connected_exempts_peripheral_from_pruning() {
        let manager = AdapterManager::default();
        let id = manager.add_peripheral(TestPeripheral::new()).id();
        expire(&manager, &id);
        manager.emit(CentralEvent::DeviceConnected(id.clone()));

        assert!(manager.prune_stale_peripherals().await.is_empty());
        assert!(manager.peripherals.get(&id).unwrap().last_seen.is_none());
    }
}

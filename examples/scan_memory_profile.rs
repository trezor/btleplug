//! Interactive memory profiling harness for BLE scanning.
//!
//! This example exists to investigate whether repeated scanning grows process
//! memory without bound, and whether `clear_peripherals` releases what it is
//! expected to release.
//!
//! Run it, leave it scanning in a busy RF environment, and take snapshots over
//! time:
//!
//! ```text
//! cargo run --example scan_memory_profile
//! ```
//!
//! Commands (type the number, press enter):
//!
//! - `1` start scanning with `ScanFilter::default()`
//! - `2` stop scanning
//! - `3` take a memory snapshot, print it, and append it to the session log
//! - `4` call `clear_peripherals`
//! - `q` quit
//!
//! Scanning starts automatically. All snapshots for one run are appended to a
//! single `scan-<unix-seconds>.log` so the whole timeline lives in one file.
//!
//! # Reading the output
//!
//! The important number is **`size_in_use`**, the total of live malloc blocks.
//! `resident_size` is what Activity Monitor shows, and it does *not* shrink
//! when memory is freed, because macOS `malloc` keeps freed blocks in
//! per-size-class caches instead of returning them to the kernel. So:
//!
//! - `size_in_use` returns to baseline after `clear_peripherals`
//!   -> nothing is leaking; the resident size is just allocator retention.
//! - `size_in_use` keeps climbing
//!   -> something really is being retained; compare `peripherals` to see
//!      whether it is btleplug's peripheral map.
//!
//! Watch `peripherals` closely. Most BLE devices rotate their addresses for
//! privacy, and CoreBluetooth reports each rotation as a brand new peripheral,
//! so this count grows over time even with a fixed set of nearby devices.
//! That growth is expected, not a leak, but it is unbounded while scanning.
//!
//! # Finding what retains memory
//!
//! In-process counters tell you *whether* memory is held, not *by whom*. For
//! that, re-run under the allocation tools:
//!
//! ```text
//! MallocStackLogging=1 cargo run --example scan_memory_profile
//! leaks <pid>
//! malloc_history <pid> --highWaterMark
//! ```
//!
//! Instruments' Allocations template works too; read the **Persistent** bytes
//! column rather than Total, and mark a generation before and after a scan.

use btleplug::api::{Central, CentralEvent, Manager as _, ScanFilter};
use btleplug::platform::{Adapter, Manager};
use futures::stream::StreamExt;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Counts of the central events seen so far, by kind.
///
/// Advertisement events are by far the most frequent thing a scan produces, so
/// these counters give the denominator for any growth: a few hundred KB spread
/// over a million advertisements is very different from the same growth over a
/// hundred.
#[derive(Default)]
struct EventCounters {
    total: AtomicU64,
    discovered: AtomicU64,
    updated: AtomicU64,
    manufacturer_data: AtomicU64,
    service_data: AtomicU64,
    services: AtomicU64,
    other: AtomicU64,
}

impl EventCounters {
    fn record(&self, event: &CentralEvent) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let counter = match event {
            CentralEvent::DeviceDiscovered(_) => &self.discovered,
            CentralEvent::DeviceUpdated(_) => &self.updated,
            CentralEvent::ManufacturerDataAdvertisement { .. } => &self.manufacturer_data,
            CentralEvent::ServiceDataAdvertisement { .. } => &self.service_data,
            CentralEvent::ServicesAdvertisement { .. } => &self.services,
            _ => &self.other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> EventTotals {
        EventTotals {
            total: self.total.load(Ordering::Relaxed),
            discovered: self.discovered.load(Ordering::Relaxed),
            updated: self.updated.load(Ordering::Relaxed),
            manufacturer_data: self.manufacturer_data.load(Ordering::Relaxed),
            service_data: self.service_data.load(Ordering::Relaxed),
            services: self.services.load(Ordering::Relaxed),
            other: self.other.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct EventTotals {
    total: u64,
    discovered: u64,
    updated: u64,
    manufacturer_data: u64,
    service_data: u64,
    services: u64,
    other: u64,
}

/// One point in time: allocator state, process state, and library state.
struct Snapshot {
    label: &'static str,
    elapsed: Duration,
    unix_seconds: u64,
    scanning: bool,
    peripherals: usize,
    events: EventTotals,
    memory: platform::MemoryStats,
}

fn main() -> anyhow::Result<()> {
    // A current-thread runtime keeps the thread count stable, so snapshots are
    // not perturbed by worker threads spinning up and down mid-measurement.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run())
}

async fn run() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let started = Instant::now();
    let session_started = unix_seconds();
    let log_path = format!("scan-{session_started}.log");
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let manager = Manager::new().await?;
    let central: Adapter = manager
        .adapters()
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no Bluetooth adapter found"))?;

    let counters = Arc::new(EventCounters::default());
    let mut events = central.events().await?;
    let event_counters = counters.clone();
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            event_counters.record(&event);
        }
    });

    // Snapshot before scanning starts, so every later delta has a baseline that
    // excludes adapter setup.
    let baseline = snapshot("baseline", started, false, &central, &counters).await?;
    let header = session_header(&log_path, session_started);
    print!("{header}");
    log.write_all(header.as_bytes())?;
    emit(
        &mut log,
        &render(&baseline, Some(&baseline), Some(&baseline)),
    )?;

    central.start_scan(ScanFilter::default()).await?;
    let mut scanning = true;
    println!("Scanning with ScanFilter::default(). Commands: 1 2 3 4 q\n");

    let mut commands = command_reader();
    let mut previous = baseline_clone(&baseline);

    while let Some(command) = commands.recv().await {
        match command.trim() {
            "1" => {
                central.start_scan(ScanFilter::default()).await?;
                scanning = true;
                println!("scan started");
            }
            "2" => {
                central.stop_scan().await?;
                scanning = false;
                println!("scan stopped");
            }
            "3" => {
                let current = snapshot("snapshot", started, scanning, &central, &counters).await?;
                let rendered = render(&current, Some(&baseline), Some(&previous));
                emit(&mut log, &rendered)?;
                println!("(appended to {log_path})");
                previous = baseline_clone(&current);
            }
            "4" => {
                central.clear_peripherals().await?;
                println!("clear_peripherals returned");
                let current = snapshot(
                    "after clear_peripherals",
                    started,
                    scanning,
                    &central,
                    &counters,
                )
                .await?;
                let rendered = render(&current, Some(&baseline), Some(&previous));
                emit(&mut log, &rendered)?;
                previous = baseline_clone(&current);
            }
            "q" | "quit" | "exit" => break,
            "" => {}
            other => println!("unknown command {other:?}; use 1, 2, 3, 4 or q"),
        }
    }

    if scanning {
        central.stop_scan().await?;
    }
    println!("log written to {log_path}");
    Ok(())
}

async fn snapshot(
    label: &'static str,
    started: Instant,
    scanning: bool,
    central: &Adapter,
    counters: &EventCounters,
) -> anyhow::Result<Snapshot> {
    Ok(Snapshot {
        label,
        elapsed: started.elapsed(),
        unix_seconds: unix_seconds(),
        scanning,
        peripherals: central.peripherals().await?.len(),
        events: counters.snapshot(),
        memory: platform::sample(),
    })
}

fn baseline_clone(snapshot: &Snapshot) -> Snapshot {
    Snapshot {
        label: snapshot.label,
        elapsed: snapshot.elapsed,
        unix_seconds: snapshot.unix_seconds,
        scanning: snapshot.scanning,
        peripherals: snapshot.peripherals,
        events: snapshot.events,
        memory: snapshot.memory.clone(),
    }
}

fn session_header(log_path: &str, session_started: u64) -> String {
    let stack_logging = std::env::var("MallocStackLogging")
        .map(|value| value != "0")
        .unwrap_or(false);
    let mut out = String::new();
    let _ = writeln!(out, "=== btleplug scan memory profile ===");
    let _ = writeln!(out, "log file       : {log_path}");
    let _ = writeln!(out, "session start  : {session_started} (unix seconds)");
    let _ = writeln!(out, "pid            : {}", std::process::id());
    let _ = writeln!(out, "MallocStackLogging: {stack_logging}");
    if !stack_logging {
        let _ = writeln!(
            out,
            "  (re-run with MallocStackLogging=1 to enable `leaks` and `malloc_history`)"
        );
    }
    let _ = writeln!(out);
    out
}

/// Renders a snapshot plus its deltas against the baseline and the previous
/// snapshot.
fn render(current: &Snapshot, baseline: Option<&Snapshot>, previous: Option<&Snapshot>) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "--- {} | t+{:.1}s | unix {} | scanning={} ---",
        current.label,
        current.elapsed.as_secs_f64(),
        current.unix_seconds,
        current.scanning
    );

    let _ = writeln!(out, "\n[btleplug]");
    let _ = writeln!(
        out,
        "  peripherals in adapter map : {}{}",
        current.peripherals,
        delta_isize(
            current.peripherals as i64,
            baseline.map(|b| b.peripherals as i64),
            previous.map(|p| p.peripherals as i64),
        )
    );

    let e = &current.events;
    let _ = writeln!(out, "\n[central events]");
    let _ = writeln!(out, "  total                      : {}", e.total);
    let _ = writeln!(out, "  DeviceDiscovered           : {}", e.discovered);
    let _ = writeln!(out, "  DeviceUpdated              : {}", e.updated);
    let _ = writeln!(
        out,
        "  ManufacturerData           : {}",
        e.manufacturer_data
    );
    let _ = writeln!(out, "  ServiceData                : {}", e.service_data);
    let _ = writeln!(out, "  Services                   : {}", e.services);
    let _ = writeln!(out, "  other                      : {}", e.other);

    out.push_str(&platform::render(
        &current.memory,
        baseline.map(|b| &b.memory),
        previous.map(|p| &p.memory),
    ));

    if e.total > 0 {
        if let Some(live) = current.memory.live_bytes() {
            let _ = writeln!(
                out,
                "\n[ratio] live malloc bytes per central event: {:.1}",
                live as f64 / e.total as f64
            );
        }
    }

    out.push('\n');
    out
}

fn emit(log: &mut File, text: &str) -> anyhow::Result<()> {
    print!("{text}");
    std::io::stdout().flush()?;
    log.write_all(text.as_bytes())?;
    log.flush()?;
    Ok(())
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Reads commands on a dedicated OS thread.
///
/// Stdin reads block, so they must not run on the runtime thread; doing so
/// would stall the event stream and distort exactly the measurements this
/// example is meant to collect.
fn command_reader() -> tokio::sync::mpsc::UnboundedReceiver<String> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}

fn delta_isize(current: i64, baseline: Option<i64>, previous: Option<i64>) -> String {
    match (baseline, previous) {
        (Some(b), Some(p)) => format!("  (baseline {:+}, prev {:+})", current - b, current - p),
        _ => String::new(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn format_delta_bytes(current: u64, earlier: u64) -> String {
    let diff = current as i128 - earlier as i128;
    let sign = if diff < 0 { "-" } else { "+" };
    format!("{sign}{}", format_bytes(diff.unsigned_abs() as u64))
}

#[cfg(target_vendor = "apple")]
mod platform {
    use super::{format_bytes, format_delta_bytes};
    use std::ffi::{CStr, c_char, c_void};
    use std::fmt::Write as _;

    /// `malloc_statistics_t` from `<malloc/malloc.h>`.
    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct MallocStatistics {
        blocks_in_use: u32,
        size_in_use: usize,
        max_size_in_use: usize,
        size_allocated: usize,
    }

    /// `mach_task_basic_info` from `<mach/task_info.h>`.
    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [i32; 2],
        system_time: [i32; 2],
        policy: i32,
        suspend_count: i32,
    }

    const MACH_TASK_BASIC_INFO: u32 = 20;
    const KERN_SUCCESS: i32 = 0;

    unsafe extern "C" {
        fn malloc_default_zone() -> *mut c_void;
        fn malloc_zone_statistics(zone: *mut c_void, stats: *mut MallocStatistics);
        fn malloc_get_zone_name(zone: *mut c_void) -> *const c_char;
        fn malloc_get_all_zones(
            task: u32,
            reader: *mut c_void,
            addresses: *mut *mut *mut c_void,
            count: *mut u32,
        ) -> i32;
        fn mach_task_self() -> u32;
        fn task_info(
            target_task: u32,
            flavor: u32,
            task_info_out: *mut i32,
            task_info_count: *mut u32,
        ) -> i32;
    }

    #[derive(Clone)]
    pub struct ZoneStats {
        pub name: String,
        pub blocks_in_use: u32,
        pub size_in_use: u64,
        pub size_allocated: u64,
    }

    #[derive(Clone)]
    pub struct MemoryStats {
        pub resident_size: u64,
        pub resident_size_max: u64,
        pub virtual_size: u64,
        pub zones: Vec<ZoneStats>,
    }

    impl MemoryStats {
        /// Total live malloc bytes across all zones.
        ///
        /// This is the figure that actually answers "is anything leaking";
        /// resident size cannot, because freed memory stays in allocator caches.
        pub fn live_bytes(&self) -> Option<u64> {
            if self.zones.is_empty() {
                None
            } else {
                Some(self.zones.iter().map(|z| z.size_in_use).sum())
            }
        }

        fn live_blocks(&self) -> u64 {
            self.zones.iter().map(|z| z.blocks_in_use as u64).sum()
        }
    }

    pub fn sample() -> MemoryStats {
        let (resident_size, resident_size_max, virtual_size) = task_basic_info();
        MemoryStats {
            resident_size,
            resident_size_max,
            virtual_size,
            zones: zone_stats(),
        }
    }

    fn task_basic_info() -> (u64, u64, u64) {
        let mut info = MachTaskBasicInfo::default();
        // The kernel expects the buffer size counted in 32-bit words.
        let mut count = (size_of::<MachTaskBasicInfo>() / size_of::<i32>()) as u32;
        let result = unsafe {
            task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as *mut i32,
                &mut count,
            )
        };
        if result == KERN_SUCCESS {
            (
                info.resident_size,
                info.resident_size_max,
                info.virtual_size,
            )
        } else {
            (0, 0, 0)
        }
    }

    fn zone_stats() -> Vec<ZoneStats> {
        let mut zones: *mut *mut c_void = std::ptr::null_mut();
        let mut count: u32 = 0;
        // task 0 and a null reader mean "this process", which lets libmalloc
        // walk its own zone list directly.
        let result =
            unsafe { malloc_get_all_zones(0, std::ptr::null_mut(), &mut zones, &mut count) };
        if result != KERN_SUCCESS || zones.is_null() {
            return fallback_zone();
        }

        let mut out = Vec::with_capacity(count as usize);
        for index in 0..count as usize {
            let zone = unsafe { *zones.add(index) };
            if zone.is_null() {
                continue;
            }
            let mut stats = MallocStatistics::default();
            unsafe { malloc_zone_statistics(zone, &mut stats) };
            let name = unsafe {
                let raw = malloc_get_zone_name(zone);
                if raw.is_null() {
                    format!("zone{index}")
                } else {
                    CStr::from_ptr(raw).to_string_lossy().into_owned()
                }
            };
            out.push(ZoneStats {
                name,
                blocks_in_use: stats.blocks_in_use,
                size_in_use: stats.size_in_use as u64,
                size_allocated: stats.size_allocated as u64,
            });
        }
        if out.is_empty() { fallback_zone() } else { out }
    }

    fn fallback_zone() -> Vec<ZoneStats> {
        let zone = unsafe { malloc_default_zone() };
        if zone.is_null() {
            return Vec::new();
        }
        let mut stats = MallocStatistics::default();
        unsafe { malloc_zone_statistics(zone, &mut stats) };
        vec![ZoneStats {
            name: "default".to_string(),
            blocks_in_use: stats.blocks_in_use,
            size_in_use: stats.size_in_use as u64,
            size_allocated: stats.size_allocated as u64,
        }]
    }

    pub fn render(
        current: &MemoryStats,
        baseline: Option<&MemoryStats>,
        previous: Option<&MemoryStats>,
    ) -> String {
        let mut out = String::new();

        let live = current.live_bytes().unwrap_or(0);
        let blocks = current.live_blocks();
        let _ = writeln!(out, "\n[malloc: live allocations]  <- the leak indicator");
        let _ = write!(out, "  size_in_use                : {}", format_bytes(live));
        if let (Some(b), Some(p)) = (baseline, previous) {
            let _ = write!(
                out,
                "  (baseline {}, prev {})",
                format_delta_bytes(live, b.live_bytes().unwrap_or(0)),
                format_delta_bytes(live, p.live_bytes().unwrap_or(0)),
            );
        }
        let _ = writeln!(out);
        let _ = write!(out, "  blocks_in_use              : {blocks}");
        if let (Some(b), Some(p)) = (baseline, previous) {
            let _ = write!(
                out,
                "  (baseline {:+}, prev {:+})",
                blocks as i64 - b.live_blocks() as i64,
                blocks as i64 - p.live_blocks() as i64,
            );
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "\n[process: resident]  <- does NOT shrink on free");
        let _ = write!(
            out,
            "  resident_size              : {}",
            format_bytes(current.resident_size)
        );
        if let (Some(b), Some(p)) = (baseline, previous) {
            let _ = write!(
                out,
                "  (baseline {}, prev {})",
                format_delta_bytes(current.resident_size, b.resident_size),
                format_delta_bytes(current.resident_size, p.resident_size),
            );
        }
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "  resident_size_max          : {}",
            format_bytes(current.resident_size_max)
        );
        let _ = writeln!(
            out,
            "  virtual_size               : {}",
            format_bytes(current.virtual_size)
        );

        let _ = writeln!(out, "\n[malloc zones]");
        let _ = writeln!(
            out,
            "  {:<24} {:>12} {:>14} {:>14}",
            "zone", "blocks", "in use", "allocated"
        );
        for zone in &current.zones {
            let _ = writeln!(
                out,
                "  {:<24} {:>12} {:>14} {:>14}",
                zone.name,
                zone.blocks_in_use,
                format_bytes(zone.size_in_use),
                format_bytes(zone.size_allocated),
            );
        }

        out
    }
}

#[cfg(not(target_vendor = "apple"))]
mod platform {
    /// Non-Apple placeholder so the example still builds everywhere.
    ///
    /// The allocator introspection this example relies on is specific to
    /// libmalloc and Mach.
    #[derive(Clone, Default)]
    pub struct MemoryStats;

    impl MemoryStats {
        pub fn live_bytes(&self) -> Option<u64> {
            None
        }
    }

    pub fn sample() -> MemoryStats {
        MemoryStats
    }

    pub fn render(
        _current: &MemoryStats,
        _baseline: Option<&MemoryStats>,
        _previous: Option<&MemoryStats>,
    ) -> String {
        "\n[memory] detailed allocation stats are only implemented on Apple platforms\n".to_string()
    }
}

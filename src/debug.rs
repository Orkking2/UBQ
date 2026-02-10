#[derive(Clone, Debug)]
pub struct LogEntry {
    pub ts_ns: u64,
    pub thread_id: u64,
    pub thread_label: Option<String>,
    pub tag: &'static str,
    pub message: String,
}

#[cfg(feature = "ubq_debug")]
mod imp {
    use super::LogEntry;
    use crossbeam_queue::SegQueue;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::fmt;
    use std::fmt::Write as FmtWrite;
    use std::fs;
    #[cfg(unix)]
    use std::io::Read;
    use std::io::{self, Write};
    #[cfg(unix)]
    use std::os::unix::io::{FromRawFd, RawFd};
    use std::panic;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, Once, OnceLock};
    use std::thread;
    use std::thread::ThreadId;
    use std::time::{Duration, Instant};

    static START: OnceLock<Instant> = OnceLock::new();
    static NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);
    static CONFIG: OnceLock<Config> = OnceLock::new();
    static LOGGER: OnceLock<Logger> = OnceLock::new();
    static LOG_TOTAL: AtomicU64 = AtomicU64::new(0);
    static PANIC_HOOK: Once = Once::new();
    #[cfg(unix)]
    static STDOUT_LOG: OnceLock<Mutex<Option<Arc<StdoutLog>>>> = OnceLock::new();
    #[cfg(unix)]
    static STDERR_LOG: OnceLock<Mutex<Option<Arc<StderrLog>>>> = OnceLock::new();

    const DEFAULT_FLUSH_MS: u64 = 200;
    const DEFAULT_BUFFER_BYTES: usize = 64 * 1024;
    const DEFAULT_RING_MAX: usize = 50_000;
    const DEFAULT_TICK_MS: u64 = 50;
    const DEFAULT_WHEEL_SLOTS: usize = 256;
    const DEFAULT_FLUSH_WAIT_MS: u64 = 500;
    const DEFAULT_SHUTDOWN_WAIT_MS: u64 = 1_000;

    thread_local! {
        static THREAD_ID: u64 = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
        static THREAD_LABEL: RefCell<Option<String>> = RefCell::new(None);
        static LOG_COUNT: Cell<u64> = Cell::new(0);
        static LAST_MAYBE_FLUSH_NS: Cell<u64> = Cell::new(0);
    }

    #[derive(Clone, Debug)]
    struct Config {
        sample_rate: u64,
        max_entries: usize,
        tag_filters: Option<Vec<String>>,
        flush_ms: u64,
        log_dir: PathBuf,
        log_path: PathBuf,
        buffer_bytes: usize,
        ring_max: usize,
        tick_ms: u64,
        wheel_slots: usize,
    }

    struct Logger {
        queue: SegQueue<LogEntry>,
        thread: OnceLock<thread::Thread>,
        thread_id: OnceLock<ThreadId>,
        handle: OnceLock<thread::JoinHandle<()>>,
        recent: Mutex<VecDeque<LogEntry>>,
        flush_request: AtomicU64,
        flush_done: AtomicU64,
        shutdown_requested: AtomicBool,
        shutdown_complete: AtomicBool,
        wheel: Mutex<TimerWheel>,
    }

    struct PeriodicTask {
        ticks: usize,
        rounds: usize,
        enabled: Arc<AtomicBool>,
        callback: Box<dyn FnMut() + Send>,
    }

    struct TimerWheel {
        slots: Vec<Vec<PeriodicTask>>,
        cursor: usize,
    }

    #[derive(Clone, Debug)]
    pub struct Registration {
        enabled: Arc<AtomicBool>,
    }

    impl Drop for Registration {
        fn drop(&mut self) {
            self.enabled.store(false, Ordering::Release);
        }
    }

    fn now_ns() -> u64 {
        START.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }

    fn config() -> &'static Config {
        CONFIG.get_or_init(|| {
            let sample_rate = std::env::var("UBQ_DEBUG_SAMPLE")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(1);
            let max_entries = std::env::var("UBQ_DEBUG_MAX")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            let tag_filters = std::env::var("UBQ_DEBUG_TAGS").ok().and_then(|value| {
                let filters: Vec<String> = value
                    .split(',')
                    .map(|part| part.trim().to_string())
                    .filter(|part| !part.is_empty())
                    .collect();
                if filters.is_empty() {
                    None
                } else {
                    Some(filters)
                }
            });

            let log_dir = std::env::var_os("UBQ_DEBUG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

            let mut log_name = std::env::var("UBQ_DEBUG_FILE")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| {
                    let base = std::env::var("UBQ_DEBUG_NAME")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                        .or_else(|| thread::current().name().map(|name| name.to_string()))
                        .unwrap_or_else(|| "ubq".to_string());
                    let mut sanitized = sanitize_label(&base);
                    if sanitized.is_empty() {
                        sanitized = "ubq".to_string();
                    }
                    sanitized
                });
            if !log_name.ends_with(".log") {
                log_name.push_str(".log");
            }
            let log_path = log_dir.join(&log_name);

            let flush_ms = std::env::var("UBQ_DEBUG_FLUSH_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_FLUSH_MS);

            let buffer_bytes = std::env::var("UBQ_DEBUG_BUFFER_BYTES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_BUFFER_BYTES);

            let ring_max = std::env::var("UBQ_DEBUG_RING")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or_else(|| if max_entries > 0 { max_entries } else { DEFAULT_RING_MAX });

            let tick_ms = std::env::var("UBQ_DEBUG_TICK_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_TICK_MS);

            let wheel_slots = std::env::var("UBQ_DEBUG_WHEEL_SLOTS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value >= 2)
                .unwrap_or(DEFAULT_WHEEL_SLOTS);

            Config {
                sample_rate,
                max_entries,
                tag_filters,
                flush_ms,
                log_dir,
                log_path,
                buffer_bytes,
                ring_max,
                tick_ms,
                wheel_slots,
            }
        })
    }

    fn logger() -> &'static Logger {
        let logger = LOGGER.get_or_init(Logger::new);
        install_panic_hook();
        logger.ensure_thread();
        logger
    }

    impl Logger {
        fn new() -> Self {
            let cfg = config();
            let wheel = TimerWheel::new(cfg.wheel_slots);
            let capacity = cfg.ring_max.min(1024).max(16);
            Logger {
                queue: SegQueue::new(),
                thread: OnceLock::new(),
                thread_id: OnceLock::new(),
                handle: OnceLock::new(),
                recent: Mutex::new(VecDeque::with_capacity(capacity)),
                flush_request: AtomicU64::new(0),
                flush_done: AtomicU64::new(0),
                shutdown_requested: AtomicBool::new(false),
                shutdown_complete: AtomicBool::new(false),
                wheel: Mutex::new(wheel),
            }
        }

        fn ensure_thread(&'static self) {
            self.handle.get_or_init(|| {
                let cfg = config();
                let logger = self;
                thread::spawn(move || writer_loop(logger, cfg))
            });
        }

        fn wake(&self) {
            if let Some(thread) = self.thread.get() {
                thread.unpark();
            }
        }

        fn request_flush(&self) -> u64 {
            let target = self.flush_request.fetch_add(1, Ordering::AcqRel) + 1;
            self.wake();
            target
        }

        fn flush_with_timeout(&self, timeout: Duration) {
            if self.is_writer_thread() {
                return;
            }
            let target = self.request_flush();
            let start = Instant::now();
            while self.flush_done.load(Ordering::Acquire) < target {
                if start.elapsed() >= timeout {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
        }

        fn is_writer_thread(&self) -> bool {
            self.thread_id
                .get()
                .map_or(false, |id| *id == thread::current().id())
        }
    }

    impl TimerWheel {
        fn new(slots: usize) -> Self {
            let slots = slots.max(2);
            let mut wheel_slots = Vec::with_capacity(slots);
            for _ in 0..slots {
                wheel_slots.push(Vec::new());
            }
            TimerWheel {
                slots: wheel_slots,
                cursor: 0,
            }
        }

        fn insert(&mut self, mut task: PeriodicTask) {
            let ticks = task.ticks.max(1);
            let slots = self.slots.len();
            let slot_offset = ticks % slots;
            let rounds = (ticks - 1) / slots;
            task.rounds = rounds;
            let slot = (self.cursor + slot_offset) % slots;
            self.slots[slot].push(task);
        }

        fn advance(&mut self) -> Vec<PeriodicTask> {
            self.cursor = (self.cursor + 1) % self.slots.len();
            let mut due = Vec::new();
            let mut slot = Vec::new();
            std::mem::swap(&mut slot, &mut self.slots[self.cursor]);
            for mut task in slot {
                if task.rounds == 0 {
                    due.push(task);
                } else {
                    task.rounds -= 1;
                    self.slots[self.cursor].push(task);
                }
            }
            due
        }
    }

    fn writer_loop(logger: &'static Logger, cfg: &'static Config) {
        let _ = logger.thread.set(thread::current());
        let _ = logger.thread_id.set(thread::current().id());

        if let Err(err) = fs::create_dir_all(&cfg.log_dir) {
            eprintln!("ubq debug: failed to create log dir {}: {err}", cfg.log_dir.display());
        }

        let mut file = match fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cfg.log_path)
        {
            Ok(file) => Some(file),
            Err(err) => {
                eprintln!(
                    "ubq debug: failed to open log file {}: {err}",
                    cfg.log_path.display()
                );
                None
            }
        };

        let mut buffer = String::with_capacity(cfg.buffer_bytes.max(1024));
        let flush_interval = Duration::from_millis(cfg.flush_ms.max(1));
        let tick = Duration::from_millis(cfg.tick_ms.max(1));
        let mut last_flush = Instant::now();
        let mut next_tick = Instant::now() + tick;

        loop {
            let shutdown_requested = logger.shutdown_requested.load(Ordering::Acquire);
            let mut batch = Vec::new();
            while let Some(entry) = logger.queue.pop() {
                batch.push(entry);
            }

            if !batch.is_empty() {
                if cfg.ring_max > 0 {
                    let mut recent = match logger.recent.lock() {
                        Ok(recent) => recent,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    for entry in batch.drain(..) {
                        write_entry(&entry, &mut buffer);
                        push_recent(&mut recent, entry, cfg.ring_max);
                    }
                } else {
                    for entry in batch.drain(..) {
                        write_entry(&entry, &mut buffer);
                    }
                }
            }

            let mut now = Instant::now();
            if !shutdown_requested && now >= next_tick {
                let mut due_tasks = Vec::new();
                while now >= next_tick {
                    let mut tasks = {
                        let mut wheel = match logger.wheel.lock() {
                            Ok(wheel) => wheel,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        wheel.advance()
                    };
                    due_tasks.append(&mut tasks);
                    next_tick += tick;
                }

                if !due_tasks.is_empty() {
                    let mut reschedule = Vec::new();
                    for mut task in due_tasks {
                        if !task.enabled.load(Ordering::Acquire) {
                            continue;
                        }
                        (task.callback)();
                        if task.enabled.load(Ordering::Acquire) {
                            reschedule.push(task);
                        }
                    }
                    if !reschedule.is_empty() {
                        let mut wheel = match logger.wheel.lock() {
                            Ok(wheel) => wheel,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        for task in reschedule {
                            wheel.insert(task);
                        }
                    }
                }
            }

            now = Instant::now();
            let requested = logger.flush_request.load(Ordering::Acquire);
            let pending_flush = requested > logger.flush_done.load(Ordering::Acquire);

            if !buffer.is_empty()
                && (buffer.len() >= cfg.buffer_bytes
                    || now.duration_since(last_flush) >= flush_interval
                    || pending_flush)
            {
                if let Some(file) = file.as_mut() {
                    let _ = file.write_all(buffer.as_bytes());
                    if now.duration_since(last_flush) >= flush_interval || pending_flush {
                        let _ = file.flush();
                    }
                }
                buffer.clear();
                last_flush = now;
            }

            if pending_flush {
                if let Some(file) = file.as_mut() {
                    let _ = file.flush();
                }
                logger.flush_done.store(requested, Ordering::Release);
            }

            if shutdown_requested && logger.queue.is_empty() && buffer.is_empty() {
                if let Some(file) = file.as_mut() {
                    let _ = file.flush();
                }
                logger.shutdown_complete.store(true, Ordering::Release);
                break;
            }

            if !pending_flush && logger.queue.is_empty() {
                let now = Instant::now();
                let mut sleep_for = next_tick.saturating_duration_since(now);
                if !buffer.is_empty() {
                    let next_flush = last_flush + flush_interval;
                    let to_flush = next_flush.saturating_duration_since(now);
                    sleep_for = sleep_for.min(to_flush);
                }
                if sleep_for.is_zero() {
                    thread::yield_now();
                } else {
                    thread::park_timeout(sleep_for);
                }
            }
        }
    }

    fn write_entry(entry: &LogEntry, buffer: &mut String) {
        let label = entry.thread_label.as_deref().unwrap_or("-");
        let _ = write!(
            buffer,
            "[{}] tid={} {} {} {}\n",
            entry.ts_ns, entry.thread_id, label, entry.tag, entry.message
        );
    }

    fn push_recent(recent: &mut VecDeque<LogEntry>, entry: LogEntry, max: usize) {
        if max == 0 {
            return;
        }
        if recent.len() >= max {
            recent.pop_front();
        }
        recent.push_back(entry);
    }

    fn should_log(tag: &'static str) -> bool {
        let cfg = config();

        if let Some(filters) = &cfg.tag_filters {
            if !filters.iter().any(|prefix| tag.starts_with(prefix)) {
                return false;
            }
        }

        if cfg.sample_rate > 1 {
            let keep = LOG_COUNT.with(|count| {
                let next = count.get().saturating_add(1);
                count.set(next);
                next % cfg.sample_rate == 0
            });
            if !keep {
                return false;
            }
        }

        if cfg.max_entries != 0 {
            let prev = LOG_TOTAL.fetch_add(1, Ordering::Relaxed);
            if prev >= cfg.max_entries as u64 {
                return false;
            }
        }

        true
    }

    pub fn init() {
        let _ = logger();
    }

    pub fn register(duration: Duration, task: impl FnMut() + Send + 'static) -> Registration {
        let logger = logger();
        let cfg = config();
        let wheel_tick = Duration::from_millis(cfg.tick_ms.max(1));
        let ticks = {
            let tick_ns = wheel_tick.as_nanos();
            let dur_ns = duration.as_nanos();
            if tick_ns == 0 {
                1
            } else {
                let ticks = (dur_ns + tick_ns - 1) / tick_ns;
                (ticks.max(1)) as usize
            }
        };

        let enabled = Arc::new(AtomicBool::new(true));
        let task = PeriodicTask {
            ticks,
            rounds: 0,
            enabled: Arc::clone(&enabled),
            callback: Box::new(task),
        };

        let mut wheel = match logger.wheel.lock() {
            Ok(wheel) => wheel,
            Err(poisoned) => poisoned.into_inner(),
        };
        wheel.insert(task);
        logger.wake();

        Registration { enabled }
    }

    pub fn set_thread_label(label: impl Into<String>) {
        let value = label.into();
        THREAD_LABEL.with(|slot| {
            *slot.borrow_mut() = Some(value);
        });
    }

    pub fn clear_thread_label() {
        THREAD_LABEL.with(|slot| {
            *slot.borrow_mut() = None;
        });
    }

    pub fn log_tagged(tag: &'static str, args: fmt::Arguments<'_>) {
        if !should_log(tag) {
            return;
        }

        let entry = LogEntry {
            ts_ns: now_ns(),
            thread_id: THREAD_ID.with(|id| *id),
            thread_label: THREAD_LABEL.with(|slot| slot.borrow().clone()),
            tag,
            message: args.to_string(),
        };

        let logger = logger();
        if logger.shutdown_requested.load(Ordering::Acquire) {
            return;
        }
        logger.queue.push(entry);
        logger.wake();
    }

    pub fn take() -> Vec<LogEntry> {
        let Some(logger) = LOGGER.get() else {
            return Vec::new();
        };
        let mut recent = match logger.recent.lock() {
            Ok(recent) => recent,
            Err(poisoned) => poisoned.into_inner(),
        };
        recent.drain(..).collect()
    }

    pub fn snapshot() -> Vec<LogEntry> {
        let Some(logger) = LOGGER.get() else {
            return Vec::new();
        };
        let recent = match logger.recent.lock() {
            Ok(recent) => recent,
            Err(poisoned) => poisoned.into_inner(),
        };
        recent.iter().cloned().collect()
    }

    pub fn thread_id() -> u64 {
        THREAD_ID.with(|id| *id)
    }

    pub fn thread_label() -> Option<String> {
        THREAD_LABEL.with(|slot| slot.borrow().clone())
    }

    pub fn elapsed_ns() -> u64 {
        now_ns()
    }

    pub fn flush() {
        let Some(logger) = LOGGER.get() else {
            return;
        };
        logger.flush_with_timeout(Duration::from_millis(DEFAULT_FLUSH_WAIT_MS));
    }

    pub fn shutdown() {
        let Some(logger) = LOGGER.get() else {
            return;
        };
        if logger.is_writer_thread() {
            logger.shutdown_requested.store(true, Ordering::Release);
            return;
        }
        logger.shutdown_requested.store(true, Ordering::Release);
        let _ = logger.request_flush();
        let start = Instant::now();
        let timeout = Duration::from_millis(DEFAULT_SHUTDOWN_WAIT_MS);
        while !logger.shutdown_complete.load(Ordering::Acquire) {
            if start.elapsed() >= timeout {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn write_to_dir(entries: &[LogEntry], dir: impl AsRef<Path>) -> io::Result<PathBuf> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let thread_id = thread_id();
        let label = thread_label();
        let file_name = if let Some(label) = label {
            let sanitized = sanitize_label(&label);
            format!("ubq-log-{thread_id}-{sanitized}.log")
        } else {
            format!("ubq-log-{thread_id}.log")
        };
        let path = dir.join(file_name);
        let mut file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        for entry in entries {
            let label = entry.thread_label.as_deref().unwrap_or("-");
            writeln!(
                file,
                "[{}] tid={} {} {} {}",
                entry.ts_ns, entry.thread_id, label, entry.tag, entry.message
            )?;
        }
        Ok(path)
    }

    pub fn flush_to_dir(dir: impl AsRef<Path>) -> io::Result<PathBuf> {
        let entries = take();
        write_to_dir(&entries, dir)
    }

    pub fn maybe_flush() {
        let Some(logger) = LOGGER.get() else {
            return;
        };
        let flush_ns = config().flush_ms.saturating_mul(1_000_000);
        if flush_ns == 0 {
            return;
        }
        let now = now_ns();
        let should_flush = LAST_MAYBE_FLUSH_NS.with(|last| {
            let prev = last.get();
            if prev == 0 || now.saturating_sub(prev) >= flush_ns {
                last.set(now);
                true
            } else {
                false
            }
        });
        if should_flush {
            logger.request_flush();
        }
    }

    #[cfg(unix)]
    struct StdoutLog {
        file: Mutex<fs::File>,
    }

    #[cfg(unix)]
    impl StdoutLog {
        fn flush(&self) {
            let mut file = match self.file.lock() {
                Ok(file) => file,
                Err(poisoned) => poisoned.into_inner(),
            };
            let _ = file.flush();
        }
    }

    #[cfg(unix)]
    fn stdout_log_slot() -> &'static Mutex<Option<Arc<StdoutLog>>> {
        STDOUT_LOG.get_or_init(|| Mutex::new(None))
    }

    fn install_panic_hook() {
        PANIC_HOOK.call_once(|| {
            let prev = panic::take_hook();
            panic::set_hook(Box::new(move |info| {
                prev(info);
                flush();
                flush_stdout_log();
                flush_stderr_log();
            }));
        });
    }

    pub fn install_stdout_panic_hook() {
        install_panic_hook();
    }

    #[cfg(unix)]
    pub fn flush_stdout_log() {
        let guard = match stdout_log_slot().try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if let Some(guard) = guard {
            if let Some(log) = guard.as_ref() {
                log.flush();
            }
        }
        let _ = io::stdout().flush();
    }

    #[cfg(unix)]
    struct StderrLog {
        file: Mutex<fs::File>,
    }

    #[cfg(unix)]
    impl StderrLog {
        fn flush(&self) {
            let mut file = match self.file.lock() {
                Ok(file) => file,
                Err(poisoned) => poisoned.into_inner(),
            };
            let _ = file.flush();
        }
    }

    #[cfg(unix)]
    fn stderr_log_slot() -> &'static Mutex<Option<Arc<StderrLog>>> {
        STDERR_LOG.get_or_init(|| Mutex::new(None))
    }

    #[cfg(unix)]
    pub fn flush_stderr_log() {
        let guard = match stderr_log_slot().try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if let Some(guard) = guard {
            if let Some(log) = guard.as_ref() {
                log.flush();
            }
        }
        let _ = io::stderr().flush();
    }

    #[cfg(not(unix))]
    pub fn flush_stdout_log() {
        let _ = io::stdout().flush();
    }

    #[cfg(not(unix))]
    pub fn flush_stderr_log() {
        let _ = io::stderr().flush();
    }

    pub fn prepare_log_dir(dir: impl AsRef<Path>) -> io::Result<()> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let log_name = log_name_for_thread();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("ubq-log-")
                || name == "stdout.log"
                || name == "stderr.log"
                || name == log_name
            {
                let path = entry.path();
                if path.is_dir() {
                    fs::remove_dir_all(path)?;
                } else {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    pub struct StdoutCapture {
        restore_fd: RawFd,
        thread: Option<thread::JoinHandle<()>>,
        _log: Arc<StdoutLog>,
    }

    #[cfg(not(unix))]
    pub struct StdoutCapture;

    #[cfg(unix)]
    pub struct StderrCapture {
        restore_fd: RawFd,
        thread: Option<thread::JoinHandle<()>>,
        _log: Arc<StderrLog>,
    }

    #[cfg(not(unix))]
    pub struct StderrCapture;

    #[cfg(unix)]
    pub fn capture_stdout(dir: impl AsRef<Path>) -> io::Result<StdoutCapture> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let log_path = dir.join("stdout.log");
        let log = Arc::new(StdoutLog {
            file: Mutex::new(fs::File::create(&log_path)?),
        });
        {
            let mut slot = match stdout_log_slot().lock() {
                Ok(slot) => slot,
                Err(poisoned) => poisoned.into_inner(),
            };
            *slot = Some(Arc::clone(&log));
        }

        let restore_fd = unsafe { dup(STDOUT_FD) };
        if restore_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        let mut fds = [0; 2];
        if unsafe { pipe(fds.as_mut_ptr()) } != 0 {
            unsafe {
                close(restore_fd);
            }
            return Err(io::Error::last_os_error());
        }

        let read_fd = fds[0];
        let write_fd = fds[1];
        if unsafe { dup2(write_fd, STDOUT_FD) } == -1 {
            unsafe {
                close(read_fd);
                close(write_fd);
                close(restore_fd);
            }
            return Err(io::Error::last_os_error());
        }
        unsafe {
            close(write_fd);
        }

        let stdout_fd = unsafe { dup(restore_fd) };
        if stdout_fd == -1 {
            unsafe {
                close(read_fd);
                close(restore_fd);
            }
            return Err(io::Error::last_os_error());
        }

        let log_thread = Arc::clone(&log);
        let thread = thread::spawn(move || {
            let mut reader = unsafe { fs::File::from_raw_fd(read_fd) };
            let mut stdout = unsafe { fs::File::from_raw_fd(stdout_fd) };
            let mut buf = [0u8; 8192];
            loop {
                let bytes = match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                let chunk = &buf[..bytes];
                if let Ok(mut file) = log_thread.file.lock() {
                    let _ = file.write_all(chunk);
                }
                let _ = stdout.write_all(chunk);
            }
            log_thread.flush();
            let _ = stdout.flush();
        });

        Ok(StdoutCapture {
            restore_fd,
            thread: Some(thread),
            _log: log,
        })
    }

    #[cfg(unix)]
    pub fn capture_stderr(dir: impl AsRef<Path>) -> io::Result<StderrCapture> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let log_path = dir.join("stderr.log");
        let log = Arc::new(StderrLog {
            file: Mutex::new(fs::File::create(&log_path)?),
        });
        {
            let mut slot = match stderr_log_slot().lock() {
                Ok(slot) => slot,
                Err(poisoned) => poisoned.into_inner(),
            };
            *slot = Some(Arc::clone(&log));
        }

        let restore_fd = unsafe { dup(STDERR_FD) };
        if restore_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        let mut fds = [0; 2];
        if unsafe { pipe(fds.as_mut_ptr()) } != 0 {
            unsafe {
                close(restore_fd);
            }
            return Err(io::Error::last_os_error());
        }

        let read_fd = fds[0];
        let write_fd = fds[1];
        if unsafe { dup2(write_fd, STDERR_FD) } == -1 {
            unsafe {
                close(read_fd);
                close(write_fd);
                close(restore_fd);
            }
            return Err(io::Error::last_os_error());
        }
        unsafe {
            close(write_fd);
        }

        let stderr_fd = unsafe { dup(restore_fd) };
        if stderr_fd == -1 {
            unsafe {
                close(read_fd);
                close(restore_fd);
            }
            return Err(io::Error::last_os_error());
        }

        let log_thread = Arc::clone(&log);
        let thread = thread::spawn(move || {
            let mut reader = unsafe { fs::File::from_raw_fd(read_fd) };
            let mut stderr = unsafe { fs::File::from_raw_fd(stderr_fd) };
            let mut buf = [0u8; 8192];
            loop {
                let bytes = match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                let chunk = &buf[..bytes];
                if let Ok(mut file) = log_thread.file.lock() {
                    let _ = file.write_all(chunk);
                }
                let _ = stderr.write_all(chunk);
            }
            log_thread.flush();
            let _ = stderr.flush();
        });

        Ok(StderrCapture {
            restore_fd,
            thread: Some(thread),
            _log: log,
        })
    }

    #[cfg(not(unix))]
    pub fn capture_stdout(_dir: impl AsRef<Path>) -> io::Result<StdoutCapture> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "stdout capture unsupported on this platform",
        ))
    }

    #[cfg(not(unix))]
    pub fn capture_stderr(_dir: impl AsRef<Path>) -> io::Result<StderrCapture> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "stderr capture unsupported on this platform",
        ))
    }

    #[cfg(unix)]
    impl Drop for StdoutCapture {
        fn drop(&mut self) {
            let _ = io::stdout().flush();
            unsafe {
                let _ = dup2(self.restore_fd, STDOUT_FD);
                close(self.restore_fd);
            }
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    #[cfg(unix)]
    impl Drop for StderrCapture {
        fn drop(&mut self) {
            let _ = io::stderr().flush();
            unsafe {
                let _ = dup2(self.restore_fd, STDERR_FD);
                close(self.restore_fd);
            }
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    #[cfg(unix)]
    const STDOUT_FD: RawFd = 1;
    #[cfg(unix)]
    const STDERR_FD: RawFd = 2;

    #[cfg(unix)]
    unsafe extern "C" {
        fn dup(fd: i32) -> i32;
        fn dup2(fd: i32, fd2: i32) -> i32;
        fn pipe(fds: *mut i32) -> i32;
        fn close(fd: i32) -> i32;
    }

    fn sanitize_label(label: &str) -> String {
        label
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect()
    }

    fn log_name_for_thread() -> String {
        let mut log_name = std::env::var("UBQ_DEBUG_FILE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                let base = std::env::var("UBQ_DEBUG_NAME")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| thread::current().name().map(|name| name.to_string()))
                    .unwrap_or_else(|| "ubq".to_string());
                let mut sanitized = sanitize_label(&base);
                if sanitized.is_empty() {
                    sanitized = "ubq".to_string();
                }
                sanitized
            });
        if !log_name.ends_with(".log") {
            log_name.push_str(".log");
        }
        log_name
    }
}

#[cfg(not(feature = "ubq_debug"))]
mod imp {
    use super::LogEntry;
    use std::fmt;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    #[derive(Clone, Debug)]
    pub struct Registration;

    pub fn init() {}

    pub fn register(_duration: Duration, _task: impl FnMut() + Send + 'static) -> Registration {
        Registration
    }

    pub fn set_thread_label(_label: impl Into<String>) {}

    pub fn clear_thread_label() {}

    pub fn log_tagged(_tag: &'static str, _args: fmt::Arguments<'_>) {}

    pub fn take() -> Vec<LogEntry> {
        Vec::new()
    }

    pub fn snapshot() -> Vec<LogEntry> {
        Vec::new()
    }

    pub fn thread_id() -> u64 {
        0
    }

    pub fn thread_label() -> Option<String> {
        None
    }

    pub fn elapsed_ns() -> u64 {
        0
    }

    pub fn flush() {}

    pub fn shutdown() {}

    pub fn write_to_dir(_entries: &[LogEntry], _dir: impl AsRef<Path>) -> io::Result<PathBuf> {
        Ok(PathBuf::new())
    }

    pub fn flush_to_dir(_dir: impl AsRef<Path>) -> io::Result<PathBuf> {
        Ok(PathBuf::new())
    }

    pub fn maybe_flush() {}

    pub fn prepare_log_dir(_dir: impl AsRef<Path>) -> io::Result<()> {
        Ok(())
    }

    pub struct StdoutCapture;
    pub struct StderrCapture;

    pub fn install_stdout_panic_hook() {}

    pub fn flush_stdout_log() {}

    pub fn flush_stderr_log() {}

    pub fn capture_stdout(_dir: impl AsRef<Path>) -> io::Result<StdoutCapture> {
        Ok(StdoutCapture)
    }

    pub fn capture_stderr(_dir: impl AsRef<Path>) -> io::Result<StderrCapture> {
        Ok(StderrCapture)
    }
}

pub use imp::*;

#[cfg(feature = "ubq_debug")]
#[macro_export]
macro_rules! log {
    (tag: $tag:expr, $($arg:tt)+) => {
        $crate::debug::log_tagged($tag, format_args!($($arg)+));
    };
    ($($arg:tt)+) => {
        $crate::debug::log_tagged("ubq", format_args!($($arg)+));
    };
}

#[cfg(not(feature = "ubq_debug"))]
#[macro_export]
macro_rules! log {
    (tag: $tag:expr, $($arg:tt)+) => {};
    ($($arg:tt)+) => {};
}

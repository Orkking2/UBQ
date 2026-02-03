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
    use std::cell::{Cell, RefCell};
    use std::fmt;
    use std::fs;
    #[cfg(unix)]
    use std::io::Read;
    use std::io::{self, Write};
    #[cfg(unix)]
    use std::os::unix::io::{FromRawFd, RawFd};
    #[cfg(unix)]
    use std::panic;
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    #[cfg(unix)]
    use std::sync::{Arc, Mutex, Once};
    #[cfg(unix)]
    use std::thread;
    use std::time::Instant;

    static START: OnceLock<Instant> = OnceLock::new();
    static NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);
    static CONFIG: OnceLock<Config> = OnceLock::new();
    #[cfg(unix)]
    static PANIC_HOOK: Once = Once::new();
    #[cfg(unix)]
    static STDOUT_LOG: OnceLock<Mutex<Option<Arc<StdoutLog>>>> = OnceLock::new();
    #[cfg(unix)]
    static STDERR_LOG: OnceLock<Mutex<Option<Arc<StderrLog>>>> = OnceLock::new();

    const DEFAULT_FLUSH_MS: u64 = 500;

    #[derive(Clone, Debug)]
    struct Config {
        sample_rate: u64,
        max_entries: usize,
        tag_filters: Option<Vec<String>>,
        flush_ms: Option<u64>,
        log_dir: Option<PathBuf>,
    }

    thread_local! {
        static THREAD_ID: u64 = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
        static THREAD_LABEL: RefCell<Option<String>> = RefCell::new(None);
        static LOGS: RefCell<Vec<LogEntry>> = RefCell::new(Vec::new());
        static LOG_COUNT: RefCell<u64> = RefCell::new(0);
        static LOG_TOTAL: RefCell<u64> = RefCell::new(0);
        static LAST_FLUSH_NS: Cell<u64> = Cell::new(0);
    }

    fn now_ns() -> u64 {
        START.get_or_init(Instant::now).elapsed().as_nanos() as u64
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
            let log_dir = std::env::var_os("UBQ_DEBUG_DIR").map(PathBuf::from);
            let flush_ms = match std::env::var("UBQ_DEBUG_FLUSH_MS") {
                Ok(value) => value.parse::<u64>().ok().filter(|value| *value > 0),
                Err(_) => log_dir.as_ref().map(|_| DEFAULT_FLUSH_MS),
            };

            Config {
                sample_rate,
                max_entries,
                tag_filters,
                flush_ms,
                log_dir,
            }
        })
    }

    fn should_log(tag: &'static str) -> bool {
        let config = config();

        if let Some(filters) = &config.tag_filters {
            if !filters.iter().any(|prefix| tag.starts_with(prefix)) {
                return false;
            }
        }

        if config.sample_rate > 1 {
            let keep = LOG_COUNT.with(|count| {
                let mut value = count.borrow_mut();
                *value += 1;
                *value % config.sample_rate == 0
            });
            if !keep {
                return false;
            }
        }

        if config.max_entries != 0 {
            let keep = LOG_TOTAL.with(|total| {
                let mut total = total.borrow_mut();
                if *total >= config.max_entries as u64 {
                    false
                } else {
                    *total += 1;
                    true
                }
            });
            if !keep {
                return false;
            }
        }

        true
    }

    pub fn log_tagged(tag: &'static str, args: fmt::Arguments<'_>) {
        if !should_log(tag) {
            return;
        }

        let message = args.to_string();
        let entry = LogEntry {
            ts_ns: now_ns(),
            thread_id: THREAD_ID.with(|id| *id),
            thread_label: THREAD_LABEL.with(|slot| slot.borrow().clone()),
            tag,
            message,
        };
        LOGS.with(|logs| logs.borrow_mut().push(entry));
    }

    pub fn take() -> Vec<LogEntry> {
        LOGS.with(|logs| logs.borrow_mut().drain(..).collect())
    }

    pub fn snapshot() -> Vec<LogEntry> {
        LOGS.with(|logs| logs.borrow().clone())
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
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
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
        let config = config();
        let Some(dir) = config.log_dir.as_ref() else {
            return;
        };
        let Some(flush_ms) = config.flush_ms else {
            return;
        };
        let flush_ns = flush_ms.saturating_mul(1_000_000);
        if flush_ns == 0 {
            return;
        }
        let now = now_ns();
        let should_flush = LAST_FLUSH_NS.with(|last| {
            let prev = last.get();
            if prev == 0 || now.saturating_sub(prev) >= flush_ns {
                last.set(now);
                true
            } else {
                false
            }
        });
        if should_flush {
            let _ = flush_to_dir(dir);
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

    #[cfg(unix)]
    pub fn install_stdout_panic_hook() {
        PANIC_HOOK.call_once(|| {
            let prev = panic::take_hook();
            panic::set_hook(Box::new(move |info| {
                prev(info);
                flush_stdout_log();
                flush_stderr_log();
            }));
        });
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
    pub fn install_stdout_panic_hook() {}

    #[cfg(not(unix))]
    pub fn flush_stdout_log() {}

    #[cfg(not(unix))]
    pub fn flush_stderr_log() {}

    pub fn prepare_log_dir(dir: impl AsRef<Path>) -> io::Result<()> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("ubq-log-") || name == "stdout.log" || name == "stderr.log" {
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
}

#[cfg(not(feature = "ubq_debug"))]
mod imp {
    use super::LogEntry;
    use std::fmt;
    use std::io;
    use std::path::{Path, PathBuf};

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
macro_rules! ubq_log {
    (tag: $tag:expr, $($arg:tt)+) => {
        $crate::debug::log_tagged($tag, format_args!($($arg)+));
    };
    ($($arg:tt)+) => {
        $crate::debug::log_tagged("ubq", format_args!($($arg)+));
    };
}

#[cfg(not(feature = "ubq_debug"))]
#[macro_export]
macro_rules! ubq_log {
    (tag: $tag:expr, $($arg:tt)+) => {};
    ($($arg:tt)+) => {};
}

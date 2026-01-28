use crossbeam_channel as cb;
use std::{
    cell::RefCell,
    fs::{OpenOptions, create_dir_all},
    io::{self, Write},
    path::PathBuf,
    sync::{
        Arc, Once, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use ubq::UBQ;

struct SwitchingFileLogger {
    sender: cb::Sender<LogCommand>,
    level: log::LevelFilter,
}

enum LogCommand {
    UpdateFile(std::fs::File),
    Record(LogEntry),
    Flush,
    FlushSync(cb::Sender<()>),
}

struct LogEntry {
    timestamp: Duration,
    thread_id: std::thread::ThreadId,
    level: log::Level,
    target: String,
    body: String,
}

impl SwitchingFileLogger {
    fn new(level: log::LevelFilter) -> Self {
        let (sender, receiver) = cb::unbounded();
        spawn_writer(receiver);

        Self { sender, level }
    }

    fn set_log_file(&self, file: std::fs::File) {
        let _ = self.sender.send(LogCommand::UpdateFile(file));
    }

    fn enqueue_flush(&self) {
        let _ = self.sender.send(LogCommand::Flush);
    }

    fn flush_sync(&self, timeout: Duration) {
        let (done_tx, done_rx) = cb::bounded(1);
        if self.sender.send(LogCommand::FlushSync(done_tx)).is_ok() {
            let _ = done_rx.recv_timeout(timeout);
        }
    }
}

impl log::Log for SwitchingFileLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let entry = LogEntry {
            timestamp,
            thread_id: std::thread::current().id(),
            level: record.level(),
            target: record.target().to_string(),
            body: record.args().to_string(),
        };

        let _ = self.sender.send(LogCommand::Record(entry));
    }

    fn flush(&self) {
        self.enqueue_flush();
    }
}

fn spawn_writer(receiver: cb::Receiver<LogCommand>) {
    thread::Builder::new()
        .name("ubq-log-writer".to_string())
        .spawn(move || writer_loop(receiver))
        .expect("failed to spawn ubq log writer thread");
}

fn writer_loop(receiver: cb::Receiver<LogCommand>) {
    let mut writer: Option<std::fs::File> = None;
    let flush_tick = cb::tick(Duration::from_millis(200));

    loop {
        cb::select! {
            recv(receiver) -> message => match message {
                Ok(LogCommand::UpdateFile(file)) => {
                    if let Some(w) = writer.as_mut() {
                        let _ = w.flush();
                    }
                    writer = Some(file);
                }
                Ok(LogCommand::Record(entry)) => {
                    if let Some(w) = writer.as_mut() {
                        let _ = writeln!(
                            w,
                            "[{:>10}.{:06}] [{:?}] {:>5} {} - {}",
                            entry.timestamp.as_secs(),
                            entry.timestamp.subsec_micros(),
                            entry.thread_id,
                            entry.level,
                            entry.target,
                            entry.body
                        );
                    }
                }
                Ok(LogCommand::Flush) => {
                    if let Some(w) = writer.as_mut() {
                        let _ = w.flush();
                    }
                }
                Ok(LogCommand::FlushSync(done)) => {
                    if let Some(w) = writer.as_mut() {
                        let _ = w.flush();
                    }
                    let _ = done.send(());
                }
                Err(_) => break,
            },
            recv(flush_tick) -> _ => {
                if let Some(w) = writer.as_mut() {
                    let _ = w.flush();
                }
            }
        }
    }
}

enum LoggerState {
    Ready(&'static SwitchingFileLogger),
    Failed(log::SetLoggerError),
}

fn logger_state() -> &'static LoggerState {
    static LOGGER: OnceLock<LoggerState> = OnceLock::new();

    LOGGER.get_or_init(|| {
        let logger = Box::leak(Box::new(SwitchingFileLogger::new(log::LevelFilter::Trace)));
        match log::set_logger(logger) {
            Ok(()) => {
                log::set_max_level(log::LevelFilter::Trace);
                LoggerState::Ready(logger)
            }
            Err(err) => LoggerState::Failed(err),
        }
    })
}

struct TraceFlushGuard {
    logger: &'static SwitchingFileLogger,
}

impl TraceFlushGuard {
    fn new(logger: &'static SwitchingFileLogger) -> Self {
        Self { logger }
    }
}

impl Drop for TraceFlushGuard {
    fn drop(&mut self) {
        self.logger.flush_sync(Duration::from_millis(500));
    }
}

thread_local! {
    static TRACE_GUARD: RefCell<Option<TraceFlushGuard>> = RefCell::new(None);
}

fn install_panic_hook() {
    static PANIC_HOOK: Once = Once::new();

    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let LoggerState::Ready(logger) = logger_state() {
                logger.flush_sync(Duration::from_millis(500));
            }
            previous(info);
        }));
    });
}

pub fn init_trace_to_file(test_name: &str) -> io::Result<PathBuf> {
    let log_dir = std::env::var("UBQ_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/ubq_logs"));
    create_dir_all(&log_dir)?;

    let path = log_dir.join(format!("{test_name}.log"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;

    install_panic_hook();
    log::set_max_level(log::LevelFilter::Trace);
    match logger_state() {
        LoggerState::Ready(logger) => {
            logger.set_log_file(file);
            TRACE_GUARD.with(|guard| {
                *guard.borrow_mut() = Some(TraceFlushGuard::new(logger));
            });
        }
        LoggerState::Failed(err) => {
            eprintln!(
                "ubq_perf: unable to install file logger ({err}); traces will use the existing logger"
            );
        }
    }

    Ok(path)
}

#[derive(Clone)]
pub struct KillSwitch {
    kill: Arc<AtomicBool>,
}

impl Drop for KillSwitch {
    fn drop(&mut self) {
        self.kill.store(false, Ordering::Relaxed);
    }
}

pub fn spawn_ubq_tracer<T: 'static>(mut ubq: UBQ<T>) -> KillSwitch {
    let atm_bool = Arc::new(AtomicBool::new(true));

    let kill_switch = KillSwitch {
        kill: atm_bool.clone(),
    };

    #[cfg(feature = "ubq_debug")]
    let _ = thread::spawn(move || {
        while atm_bool.load(Ordering::Relaxed) {
            log::warn!("Snapshot: {:?}", ubq.debug_state());
            thread::sleep(Duration::from_millis(5));
        }
    });

    #[cfg(not(feature = "ubq_debug"))]
    let _ = ubq;

    kill_switch
}

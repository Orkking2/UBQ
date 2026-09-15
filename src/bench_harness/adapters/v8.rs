use super::*;

/// Shared adapter for v8 and derivatives with owned-buffer batch operations.
/// Each thread keeps its own buffers for reuse after warmup. `B` is the
/// queue's backoff policy, chosen by the workload like UBQ's.
pub(super) struct Queue<B> {
    queue: implementation::UBQ<u64, B>,
}

impl<B: backoff::BackoffPolicy + 'static> Queue<B> {
    /// An omitted dimension keeps the implementation's default.
    pub(super) fn new(
        slots_per_vblock: Option<usize>,
        vblocks_per_block: Option<usize>,
    ) -> Result<Arc<Self>, String> {
        let default = implementation::Config::default();
        let config = implementation::Config {
            slots_per_vblock: slots_per_vblock.unwrap_or(default.slots_per_vblock),
            vblocks_per_block: vblocks_per_block.unwrap_or(default.vblocks_per_block),
        };
        let queue = implementation::UBQ::with_config(config).map_err(|e| e.to_string())?;
        Ok(Arc::new(Self { queue }))
    }
}

pub(super) struct Handle<B> {
    queue: Arc<Queue<B>>,
    send: UnsafeCell<Vec<u64>>,
    recv: UnsafeCell<Option<Box<[std::mem::MaybeUninit<u64>]>>>,
}

impl<B: backoff::BackoffPolicy + 'static> Handle<B> {
    fn recv_batch<R>(&self, size: usize, inspect: impl FnOnce(&[u64]) -> R) -> R {
        // SAFETY: as for `LubqThreadHandle::role_mut`, each handle is owned by
        // one worker and its operations never overlap.
        let slot = unsafe { &mut *self.recv.get() };
        let buf = match slot.take() {
            Some(buf) if buf.len() == size => buf,
            _ => Box::new_uninit_slice(size),
        };
        let mut items = self.queue.queue.pop_batch(buf);
        let result = inspect(&items);
        items.clear();
        *slot = implementation::queue::empty_vec_to_box_uninit(items).ok();
        result
    }
}

impl<B: backoff::BackoffPolicy + 'static> BenchQueueHandleFactory for Queue<B> {
    type ThreadHandle = Handle<B>;

    fn thread_handle(self: &Arc<Self>) -> Self::ThreadHandle {
        Handle {
            queue: self.clone(),
            send: UnsafeCell::new(Vec::new()),
            recv: UnsafeCell::new(None),
        }
    }
}

impl<B: backoff::BackoffPolicy + 'static> BenchQueueThreadOps for Handle<B> {
    fn visit_recv_batch(&self, size: usize, visit: &mut dyn FnMut(u64)) -> usize {
        self.recv_batch(size, |items| {
            items.iter().for_each(|value| visit(*value));
            items.len()
        })
    }
    fn try_send_value(&self, value: u64) -> bool {
        self.queue.queue.push(value);
        true
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        // SAFETY: see `recv_batch`.
        let buf = unsafe { &mut *self.send.get() };
        buf.extend(offsets.map(move |offset| base + offset as u64));
        *buf = self.queue.queue.push_batch(std::mem::take(buf));
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.queue.queue.pop()
    }

    fn try_recv_batch(&self, request_size: usize) -> usize {
        self.recv_batch(request_size, <[u64]>::len)
    }
}

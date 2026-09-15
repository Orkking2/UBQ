use super::*;

pub(super) struct Queue<B> {
    queue: implementation::UBQ<u64, B>,
}

impl<B: backoff::BackoffPolicy + 'static> Queue<B> {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: implementation::UBQ::new(),
        })
    }
}

impl<B: backoff::BackoffPolicy + 'static> BenchQueueOps for Queue<B> {
    fn visit_recv_batch(&self, size: usize, visit: &mut dyn FnMut(u64)) -> usize {
        self.queue
            .pop_batch(size)
            .inspect(|value| visit(*value))
            .count()
    }

    fn try_send_value(&self, value: u64) -> bool {
        self.queue.push(value);
        true
    }

    fn send_batch(&self, base: u64, offsets: std::ops::Range<usize>) {
        self.queue
            .push_batch(offsets.map(move |offset| base + offset as u64));
    }

    fn try_recv_value(&self) -> Option<u64> {
        self.queue.pop()
    }

    fn try_recv_batch(&self, size: usize) -> usize {
        self.queue.pop_batch(size).count()
    }
}

//! Standalone harness control, compiled by rustc without the unfinished library.
//! Passing this target validates the test fixtures, never v8 itself.
use std::{collections::VecDeque, mem::MaybeUninit, sync::Mutex};

pub struct Config {
    pub slots_per_vblock: usize,
    pub vblocks_per_block: usize,
}

pub struct Queue<T>(Mutex<VecDeque<T>>);

impl<T> Queue<T> {
    pub fn with_config(config: Config) -> Result<Self, &'static str> {
        if config.slots_per_vblock == 0 || config.vblocks_per_block == 0 {
            return Err("zero capacity");
        }
        Ok(Self(Mutex::new(VecDeque::new())))
    }

    pub fn push(&self, value: T) {
        self.0.lock().unwrap().push_back(value);
    }

    pub fn push_batch(&self, mut values: Vec<T>) -> Vec<T> {
        self.0.lock().unwrap().extend(values.drain(..));
        values
    }

    pub fn pop(&self) -> Option<T> {
        self.0.lock().unwrap().pop_front()
    }

    pub fn pop_batch(&self, buffer: Box<[MaybeUninit<T>]>) -> Vec<T> {
        let mut queue = self.0.lock().unwrap();
        let mut buffer = buffer.into_vec();
        let count = buffer.len().min(queue.len());
        for slot in &mut buffer[..count] {
            slot.write(queue.pop_front().unwrap());
        }
        let (ptr, _, capacity) = buffer.into_raw_parts();
        // SAFETY: exactly the first `count` elements are initialized above;
        // MaybeUninit<T> has T's layout, and this transfers the Vec allocation.
        unsafe { Vec::from_raw_parts(ptr.cast(), count, capacity) }
    }

    pub fn flush_freelist(&self) -> usize {
        0
    }
}

#[path = "v8_cases.rs"]
mod cases;

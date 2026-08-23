use core::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{
        AtomicU8,
        Ordering::{Acquire, Release},
    },
};

use crate::backoff::BackoffPolicy;

const ZERO: u8 = 0;
const WRITTEN: u8 = 1;
const SKIP: u8 = 2;

pub struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
}

impl<T> Slot<T> {
    pub(crate) const fn new() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicU8::new(ZERO),
        }
    }

    fn acquire_state(&self) -> u8 {
        self.state.load(Acquire)
    }

    fn release_state(&self, state: u8) {
        self.state.store(state, Release);
    }

    pub fn reset(&mut self) {
        *self.state.get_mut() = ZERO;
    }

    pub fn read<B>(&self, backoff: &B) -> Option<T>
    where
        B: BackoffPolicy,
    {
        loop {
            let state = self.acquire_state();

            match state {
                WRITTEN => break Some(unsafe { self.value.get().read().assume_init() }),
                SKIP => break None,
                ZERO => {} // continue
                _ => unreachable!(),
            }

            backoff.snooze();
        }
    }

    pub fn skip(&self) {
        self.release_state(SKIP);
    }

    pub fn write(&self, val: T) {
        unsafe { self.value.get().write(MaybeUninit::new(val)) }
        self.release_state(WRITTEN);
    }

    pub fn drop_inner(&mut self) {
        if *self.state.get_mut() == WRITTEN {
            unsafe { self.value.get_mut().assume_init_drop() }
        }
    }
}

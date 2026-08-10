use crate::backoff::BackoffPolicy;
use core::{cell::UnsafeCell, mem::MaybeUninit};
use portable_atomic::{AtomicU8, Ordering};

pub(crate) struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
}

const ZERO: u8 = 0;
const WRITE: u8 = 1;
const SKIP: u8 = 2;
const READ: u8 = SKIP;

impl<T> Drop for Slot<T> {
    fn drop(&mut self) {
        if *self.state.get_mut() == WRITE {
            unsafe { self.value.get_mut().assume_init_drop() };
        }
    }
}

impl<T> Slot<T> {
    pub fn new() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicU8::new(ZERO),
        }
    }

    pub(crate) fn write_opt(&self, e_opt: Option<T>) {
        if let Some(e) = e_opt {
            self.write(e);
        } else {
            self.skip();
        }
    }

    fn write(&self, e: T) {
        unsafe {
            self.value.get().write(MaybeUninit::new(e));
        }
        self.store_state(WRITE);
    }

    fn skip(&self) {
        self.store_state(SKIP);
    }

    fn store_state(&self, val: u8) {
        self.state.store(val, Ordering::Release);
    }

    pub(crate) fn read<B: BackoffPolicy>(&self, backoff: &B) -> Option<T> {
        let state = self.await_nonzero(backoff);

        if state == WRITE {
            let out = unsafe { self.value.get().read() };
            self.store_state(READ);

            Some(unsafe { out.assume_init() })
        } else if state == SKIP {
            None
        } else {
            unreachable!("state must either be ZERO, WRITE, or SKIP");
        }
    }

    fn await_nonzero<B: BackoffPolicy>(&self, backoff: &B) -> u8 {
        loop {
            let state = self.state.load(Ordering::Acquire);

            if state != ZERO {
                break state;
            }

            backoff.snooze();
        }
    }

    pub(crate) fn reset(self: &mut Self) {
        *self.state.get_mut() = ZERO;
    }
}

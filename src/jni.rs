use crate::{UBQ, backoff};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[cfg(feature = "bench_fastfifo")]
use rbbq::FastFifo;

type JClass = *mut std::ffi::c_void;
type JInt = i32;
type JLong = i64;
type JNIEnv = *mut std::ffi::c_void;
type JBoolean = u8;

const JNI_TRUE: JBoolean = 1;
const JNI_FALSE: JBoolean = 0;

const UBQ_VARIANT_COUNT: JInt = 6;
const DEFAULT_UBQ_VARIANT_ID: JInt = 1;

#[cfg(test)]
const UBQ_VARIANT_LABELS: &[&str] = &[
    "balanced,1,63,crossbeam",
    "balanced,1,127,crossbeam",
    "balanced,1,255,crossbeam",
    "balanced,1,31,crossbeam",
    "balanced,1,511,crossbeam",
    "balanced,1,127,yield",
];

enum JniQueue {
    Balanced1Block63Crossbeam(UBQ<u64, 63, backoff::Crossbeam>),
    Balanced1Block127Crossbeam(UBQ<u64, 127, backoff::Crossbeam>),
    Balanced1Block255Crossbeam(UBQ<u64, 255, backoff::Crossbeam>),
    Balanced1Block31Crossbeam(UBQ<u64, 31, backoff::Crossbeam>),
    Balanced1Block511Crossbeam(UBQ<u64, 511, backoff::Crossbeam>),
    Balanced1Block127Yield(UBQ<u64, 127, backoff::Yield>),
}

macro_rules! with_ubq {
    ($queue:expr, $inner:ident, $body:block) => {
        match $queue {
            JniQueue::Balanced1Block63Crossbeam($inner) => $body,
            JniQueue::Balanced1Block127Crossbeam($inner) => $body,
            JniQueue::Balanced1Block255Crossbeam($inner) => $body,
            JniQueue::Balanced1Block31Crossbeam($inner) => $body,
            JniQueue::Balanced1Block511Crossbeam($inner) => $body,
            JniQueue::Balanced1Block127Yield($inner) => $body,
        }
    };
}

#[cfg(feature = "bench_fastfifo")]
struct JniRbbqQueue {
    queue: FastFifo<u64>,
}

impl JniQueue {
    fn new(variant_id: JInt) -> Option<Self> {
        match variant_id {
            0 => Some(Self::Balanced1Block63Crossbeam(UBQ::new())),
            1 => Some(Self::Balanced1Block127Crossbeam(UBQ::new())),
            2 => Some(Self::Balanced1Block255Crossbeam(UBQ::new())),
            3 => Some(Self::Balanced1Block31Crossbeam(UBQ::new())),
            4 => Some(Self::Balanced1Block511Crossbeam(UBQ::new())),
            5 => Some(Self::Balanced1Block127Yield(UBQ::new())),
            _ => None,
        }
    }
}

#[cfg(feature = "bench_fastfifo")]
impl JniRbbqQueue {
    fn new(capacity: JLong, block_size: JInt) -> Option<Self> {
        let capacity = usize::try_from(capacity).ok()?;
        let block_size = usize::try_from(block_size).ok()?;
        if block_size == 0 {
            return None;
        }
        let num_blocks = capacity.div_ceil(block_size).saturating_add(2).max(2);
        Some(Self {
            queue: FastFifo::new(num_blocks, block_size),
        })
    }
}

fn handle_to_queue<'a>(handle: JLong) -> Option<&'a JniQueue> {
    if handle == 0 {
        return None;
    }

    Some(unsafe { &*(handle as *const JniQueue) })
}

#[cfg(feature = "bench_fastfifo")]
fn handle_to_rbbq_queue<'a>(handle: JLong) -> Option<&'a JniRbbqQueue> {
    if handle == 0 {
        return None;
    }

    Some(unsafe { &*(handle as *const JniRbbqQueue) })
}

fn catch_or<T>(default: T, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(default)
}

fn valid_count(count: JLong) -> Option<usize> {
    usize::try_from(count).ok()
}

fn pop_exact(handle: JLong, count: JLong, mut apply: impl FnMut(JLong, JLong) -> JLong) -> JLong {
    catch_or(0, || {
        let Some(count) = valid_count(count) else {
            return 0;
        };
        let Some(queue) = handle_to_queue(handle) else {
            return 0;
        };

        with_ubq!(queue, inner, {
            let mut value = 0;
            for _ in 0..count {
                let item = loop {
                    if let Some(item) = inner.pop() {
                        break item as JLong;
                    }
                    std::hint::spin_loop();
                };
                value = apply(value, item);
            }
            value
        })
    })
}

#[cfg(feature = "bench_fastfifo")]
fn rbbq_push_blocking(queue: &FastFifo<u64>, value: u64) {
    while queue.push(value).is_err() {
        std::hint::spin_loop();
    }
}

#[cfg(feature = "bench_fastfifo")]
fn rbbq_pop_blocking(queue: &FastFifo<u64>) -> u64 {
    loop {
        if let Ok(item) = queue.pop() {
            return item;
        }
        std::hint::spin_loop();
    }
}

#[cfg(feature = "bench_fastfifo")]
fn rbbq_pop_exact(
    handle: JLong,
    count: JLong,
    mut apply: impl FnMut(JLong, JLong) -> JLong,
) -> JLong {
    catch_or(0, || {
        let Some(count) = valid_count(count) else {
            return 0;
        };
        let Some(queue) = handle_to_rbbq_queue(handle) else {
            return 0;
        };

        let mut value = 0;
        for _ in 0..count {
            value = apply(value, rbbq_pop_blocking(&queue.queue) as JLong);
        }
        value
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativeCreate(_env: JNIEnv, _class: JClass) -> JLong {
    Java_ubq_jni_UbqLongQueue_nativeCreateVariant(null_env(), null_class(), DEFAULT_UBQ_VARIANT_ID)
}

fn null_env() -> JNIEnv {
    std::ptr::null_mut()
}

fn null_class() -> JClass {
    std::ptr::null_mut()
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativeCreateVariant(
    _env: JNIEnv,
    _class: JClass,
    variant_id: JInt,
) -> JLong {
    catch_or(0, || {
        JniQueue::new(variant_id)
            .map(|queue| Box::into_raw(Box::new(queue)) as JLong)
            .unwrap_or(0)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativeVariantCount(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    UBQ_VARIANT_COUNT
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativeDefaultVariantId(
    _env: JNIEnv,
    _class: JClass,
) -> JInt {
    DEFAULT_UBQ_VARIANT_ID
}

#[unsafe(no_mangle)]
unsafe extern "system" fn Java_ubq_jni_UbqLongQueue_nativeDestroy(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut JniQueue));
            }
        }
    }));
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePush(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    value: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(queue) = handle_to_queue(handle) {
            with_ubq!(queue, inner, {
                inner.push(value as u64);
            });
        }
    }));
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePushRange(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    first_value: JLong,
    count: JInt,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if count <= 0 {
            return;
        }

        if let Some(queue) = handle_to_queue(handle) {
            with_ubq!(queue, inner, {
                for offset in 0..count {
                    inner.push(first_value.wrapping_add(offset as JLong) as u64);
                }
            });
        }
    }));
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePushConstantExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    value: JLong,
    count: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(count) = valid_count(count) else {
            return;
        };

        if let Some(queue) = handle_to_queue(handle) {
            with_ubq!(queue, inner, {
                for _ in 0..count {
                    inner.push(value as u64);
                }
            });
        }
    }));
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePushRangeExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    first_value: JLong,
    count: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(count) = valid_count(count) else {
            return;
        };

        if let Some(queue) = handle_to_queue(handle) {
            with_ubq!(queue, inner, {
                for offset in 0..count {
                    inner.push(first_value.wrapping_add(offset as JLong) as u64);
                }
            });
        }
    }));
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePushRangeToThreeExact(
    _env: JNIEnv,
    _class: JClass,
    first_handle: JLong,
    second_handle: JLong,
    third_handle: JLong,
    first_value: JLong,
    count: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(count) = valid_count(count) else {
            return;
        };
        let Some(first) = handle_to_queue(first_handle) else {
            return;
        };
        let Some(second) = handle_to_queue(second_handle) else {
            return;
        };
        let Some(third) = handle_to_queue(third_handle) else {
            return;
        };

        with_ubq!(first, first_inner, {
            with_ubq!(second, second_inner, {
                with_ubq!(third, third_inner, {
                    for offset in 0..count {
                        let value = first_value.wrapping_add(offset as JLong) as u64;
                        first_inner.push(value);
                        second_inner.push(value);
                        third_inner.push(value);
                    }
                });
            });
        });
    }));
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePopOr(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    empty_value: JLong,
) -> JLong {
    catch_or(empty_value, || {
        let Some(queue) = handle_to_queue(handle) else {
            return empty_value;
        };

        with_ubq!(queue, inner, {
            inner
                .pop()
                .map(|value| value as JLong)
                .unwrap_or(empty_value)
        })
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePopAddExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    count: JLong,
) -> JLong {
    pop_exact(handle, count, JLong::wrapping_add)
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePopSubExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    count: JLong,
) -> JLong {
    pop_exact(handle, count, JLong::wrapping_sub)
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePopAndExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    count: JLong,
) -> JLong {
    pop_exact(handle, count, |lhs, rhs| lhs & rhs)
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativePopBatch(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    limit: JInt,
) -> JInt {
    catch_or(0, || {
        if limit <= 0 {
            return 0;
        }

        let Some(queue) = handle_to_queue(handle) else {
            return 0;
        };

        with_ubq!(queue, inner, {
            inner.pop_batch(limit as usize).count() as JInt
        })
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_UbqLongQueue_nativeIsEmpty(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
) -> JBoolean {
    catch_or(JNI_TRUE, || {
        let Some(queue) = handle_to_queue(handle) else {
            return JNI_TRUE;
        };

        with_ubq!(queue, inner, {
            if inner.is_empty() {
                JNI_TRUE
            } else {
                JNI_FALSE
            }
        })
    })
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
    capacity: JLong,
    block_size: JInt,
) -> JLong {
    catch_or(0, || {
        JniRbbqQueue::new(capacity, block_size)
            .map(|queue| Box::into_raw(Box::new(queue)) as JLong)
            .unwrap_or(0)
    })
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
unsafe extern "system" fn Java_ubq_jni_RbbqLongQueue_nativeDestroy(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut JniRbbqQueue));
            }
        }
    }));
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativePushConstantExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    value: JLong,
    count: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(count) = valid_count(count) else {
            return;
        };

        if let Some(queue) = handle_to_rbbq_queue(handle) {
            for _ in 0..count {
                rbbq_push_blocking(&queue.queue, value as u64);
            }
        }
    }));
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativePushRangeExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    first_value: JLong,
    count: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(count) = valid_count(count) else {
            return;
        };

        if let Some(queue) = handle_to_rbbq_queue(handle) {
            for offset in 0..count {
                rbbq_push_blocking(
                    &queue.queue,
                    first_value.wrapping_add(offset as JLong) as u64,
                );
            }
        }
    }));
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativePushRangeToThreeExact(
    _env: JNIEnv,
    _class: JClass,
    first_handle: JLong,
    second_handle: JLong,
    third_handle: JLong,
    first_value: JLong,
    count: JLong,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let Some(count) = valid_count(count) else {
            return;
        };
        let Some(first) = handle_to_rbbq_queue(first_handle) else {
            return;
        };
        let Some(second) = handle_to_rbbq_queue(second_handle) else {
            return;
        };
        let Some(third) = handle_to_rbbq_queue(third_handle) else {
            return;
        };

        for offset in 0..count {
            let value = first_value.wrapping_add(offset as JLong) as u64;
            rbbq_push_blocking(&first.queue, value);
            rbbq_push_blocking(&second.queue, value);
            rbbq_push_blocking(&third.queue, value);
        }
    }));
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativePopAddExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    count: JLong,
) -> JLong {
    rbbq_pop_exact(handle, count, JLong::wrapping_add)
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativePopSubExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    count: JLong,
) -> JLong {
    rbbq_pop_exact(handle, count, JLong::wrapping_sub)
}

#[cfg(feature = "bench_fastfifo")]
#[unsafe(no_mangle)]
extern "system" fn Java_ubq_jni_RbbqLongQueue_nativePopAndExact(
    _env: JNIEnv,
    _class: JClass,
    handle: JLong,
    count: JLong,
) -> JLong {
    rbbq_pop_exact(handle, count, |lhs, rhs| lhs & rhs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr::null_mut;

    #[test]
    fn jni_exports_push_and_pop_values() {
        let handle = Java_ubq_jni_UbqLongQueue_nativeCreate(null_mut(), null_mut());
        assert_ne!(handle, 0);

        Java_ubq_jni_UbqLongQueue_nativePush(null_mut(), null_mut(), handle, 7);
        Java_ubq_jni_UbqLongQueue_nativePush(null_mut(), null_mut(), handle, 8);

        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopOr(null_mut(), null_mut(), handle, -1),
            7
        );
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopOr(null_mut(), null_mut(), handle, -1),
            8
        );
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopOr(null_mut(), null_mut(), handle, -1),
            -1
        );

        unsafe {
            Java_ubq_jni_UbqLongQueue_nativeDestroy(null_mut(), null_mut(), handle);
        }
    }

    #[test]
    fn jni_exports_range_push_and_batch_pop() {
        let handle = Java_ubq_jni_UbqLongQueue_nativeCreate(null_mut(), null_mut());
        assert_ne!(handle, 0);

        Java_ubq_jni_UbqLongQueue_nativePushRange(null_mut(), null_mut(), handle, 10, 5);

        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopBatch(null_mut(), null_mut(), handle, 3),
            3
        );
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopOr(null_mut(), null_mut(), handle, -1),
            13
        );
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopOr(null_mut(), null_mut(), handle, -1),
            14
        );
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopOr(null_mut(), null_mut(), handle, -1),
            -1
        );

        unsafe {
            Java_ubq_jni_UbqLongQueue_nativeDestroy(null_mut(), null_mut(), handle);
        }
    }

    #[test]
    fn jni_exports_exact_native_loops() {
        let handle = Java_ubq_jni_UbqLongQueue_nativeCreate(null_mut(), null_mut());
        assert_ne!(handle, 0);

        Java_ubq_jni_UbqLongQueue_nativePushConstantExact(null_mut(), null_mut(), handle, 3, 4);
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopAddExact(null_mut(), null_mut(), handle, 4),
            12
        );

        Java_ubq_jni_UbqLongQueue_nativePushRangeExact(null_mut(), null_mut(), handle, 1, 4);
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopSubExact(null_mut(), null_mut(), handle, 4),
            -10
        );

        Java_ubq_jni_UbqLongQueue_nativePushRangeExact(null_mut(), null_mut(), handle, 1, 4);
        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativePopAndExact(null_mut(), null_mut(), handle, 4),
            0
        );

        unsafe {
            Java_ubq_jni_UbqLongQueue_nativeDestroy(null_mut(), null_mut(), handle);
        }
    }

    #[test]
    fn jni_exports_all_configured_ubq_variants() {
        assert_eq!(
            UBQ_VARIANT_LABELS[DEFAULT_UBQ_VARIANT_ID as usize],
            "balanced,1,127,crossbeam"
        );

        for variant_id in 0..UBQ_VARIANT_LABELS.len() as JInt {
            let handle =
                Java_ubq_jni_UbqLongQueue_nativeCreateVariant(null_mut(), null_mut(), variant_id);
            assert_ne!(handle, 0, "variant {variant_id}");

            Java_ubq_jni_UbqLongQueue_nativePushRangeExact(null_mut(), null_mut(), handle, 1, 4);
            assert_eq!(
                Java_ubq_jni_UbqLongQueue_nativePopAddExact(null_mut(), null_mut(), handle, 4),
                10,
                "variant {variant_id}"
            );

            unsafe {
                Java_ubq_jni_UbqLongQueue_nativeDestroy(null_mut(), null_mut(), handle);
            }
        }

        assert_eq!(
            Java_ubq_jni_UbqLongQueue_nativeCreateVariant(
                null_mut(),
                null_mut(),
                UBQ_VARIANT_LABELS.len() as JInt
            ),
            0
        );
    }

    #[cfg(feature = "bench_fastfifo")]
    #[test]
    fn jni_exports_rbbq_exact_native_loops() {
        let handle = Java_ubq_jni_RbbqLongQueue_nativeCreate(null_mut(), null_mut(), 64, 8);
        assert_ne!(handle, 0);

        Java_ubq_jni_RbbqLongQueue_nativePushConstantExact(null_mut(), null_mut(), handle, 3, 4);
        assert_eq!(
            Java_ubq_jni_RbbqLongQueue_nativePopAddExact(null_mut(), null_mut(), handle, 4),
            12
        );

        Java_ubq_jni_RbbqLongQueue_nativePushRangeExact(null_mut(), null_mut(), handle, 1, 4);
        assert_eq!(
            Java_ubq_jni_RbbqLongQueue_nativePopSubExact(null_mut(), null_mut(), handle, 4),
            -10
        );

        Java_ubq_jni_RbbqLongQueue_nativePushRangeExact(null_mut(), null_mut(), handle, 1, 4);
        assert_eq!(
            Java_ubq_jni_RbbqLongQueue_nativePopAndExact(null_mut(), null_mut(), handle, 4),
            0
        );

        unsafe {
            Java_ubq_jni_RbbqLongQueue_nativeDestroy(null_mut(), null_mut(), handle);
        }
    }
}

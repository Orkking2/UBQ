use core::sync::atomic::{
    AtomicUsize,
    Ordering::{AcqRel, Acquire},
};

const UNINITIALIZED: usize = 0;

/// Returns the operating system's base-page size.
///
/// This is deliberately not the huge-page size. A normal global-allocator
/// allocation does not opt into explicit huge pages, and the base-page size is
/// the alignment used by the packed queue-head representations.
pub(crate) fn page_size() -> usize {
    static PAGE_SIZE: AtomicUsize = AtomicUsize::new(UNINITIALIZED);

    let cached = PAGE_SIZE.load(Acquire);
    if cached != UNINITIALIZED {
        return cached;
    }

    let queried = query_page_size();
    assert!(
        queried.is_power_of_two(),
        "the system page size must be a power of two"
    );

    match PAGE_SIZE.compare_exchange(UNINITIALIZED, queried, AcqRel, Acquire) {
        Ok(_) => queried,
        Err(cached) => cached,
    }
}

#[cfg(unix)]
fn query_page_size() -> usize {
    // SAFETY: sysconf has no pointer arguments. `_SC_PAGESIZE` is a read-only
    // process-wide query.
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(value).expect("sysconf(_SC_PAGESIZE) failed")
}

#[cfg(windows)]
fn query_page_size() -> usize {
    use core::mem::MaybeUninit;
    use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};

    let mut info = MaybeUninit::<SYSTEM_INFO>::uninit();
    // SAFETY: GetSystemInfo initializes the caller-provided SYSTEM_INFO.
    unsafe { GetSystemInfo(info.as_mut_ptr()) };
    // SAFETY: GetSystemInfo has initialized every field.
    usize::try_from(unsafe { info.assume_init() }.dwPageSize)
        .expect("the system page size does not fit in usize")
}

// WebAssembly's linear-memory page is fixed by the platform specification.
#[cfg(all(not(unix), not(windows), target_family = "wasm"))]
fn query_page_size() -> usize {
    65_536
}

#[cfg(not(any(unix, windows, target_family = "wasm")))]
compile_error!("ubq needs a target-specific base-page-size implementation");

#[cfg(test)]
mod tests {
    use super::page_size;

    #[test]
    fn page_size_is_usable_as_a_layout_alignment() {
        let size = page_size();
        assert!(size.is_power_of_two());
        assert!(core::alloc::Layout::from_size_align(size, size).is_ok());
    }
}

//! NUMA placement for `bench_atomic_updates`' shared counter.
//!
//! On a multi-socket machine the node that holds the counter's page decides
//! which update method wins (see `docs/atomic_update_fairness.md`), so the
//! benchmark controls and records it instead of leaving it to whichever
//! thread first touches a heap page.
//!
//! The counter lives in an anonymous mapping of its own. When a node is
//! requested, the mapping is bound to that node with `mbind` before the first
//! write, so the kernel allocates the page there, and automatic NUMA balancing
//! leaves bound pages in place. Without a node the first write decides, as for
//! any allocation. Either way `get_mempolicy` reports the node that actually
//! holds the page, and the benchmark reads it before and after every trial.

use serde::Serialize;
use std::fmt;
use std::ops::Deref;
use std::ptr::NonNull;
use std::str::FromStr;

/// Where trials place their counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Placement {
    /// No binding: the page lands wherever the first write puts it.
    Unplaced,
    /// Bound to the node of the CPU the first updater is pinned to.
    Local,
    /// Bound to this node.
    Node(usize),
}

impl FromStr for Placement {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "unplaced" | "unbound" => Ok(Self::Unplaced),
            "local" => Ok(Self::Local),
            other => other.parse().map(Self::Node).map_err(|_| {
                format!("expected `unplaced`, `local`, or a NUMA node number, not `{value}`")
            }),
        }
    }
}

impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unplaced => f.write_str("unplaced"),
            Self::Local => f.write_str("local"),
            Self::Node(node) => write!(f, "node{node}"),
        }
    }
}

/// One NUMA node and the CPUs on it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NodeCpus {
    pub node: usize,
    pub cpus: Vec<usize>,
}

/// Parses a kernel CPU list such as `0-3,8,10-11`.
pub fn parse_cpu_list(list: &str) -> Option<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in list.trim().split(',').filter(|part| !part.is_empty()) {
        match part.split_once('-') {
            Some((first, last)) => {
                let (first, last) = (first.parse().ok()?, last.parse::<usize>().ok()?);
                if first > last {
                    return None;
                }
                cpus.extend(first..=last);
            }
            None => cpus.push(part.parse().ok()?),
        }
    }
    Some(cpus)
}

/// The machine's NUMA nodes, or none where the topology is not exposed.
pub fn nodes() -> Vec<NodeCpus> {
    let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") else {
        return Vec::new();
    };
    let mut nodes = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let node = entry
                .file_name()
                .to_str()?
                .strip_prefix("node")?
                .parse()
                .ok()?;
            let list = std::fs::read_to_string(entry.path().join("cpulist")).ok()?;
            Some(NodeCpus {
                node,
                cpus: parse_cpu_list(&list)?,
            })
        })
        .collect::<Vec<_>>();
    nodes.sort_by_key(|node| node.node);
    nodes
}

/// How a sample's counter sat relative to its updaters.
pub fn relation(counter_node: Option<usize>, updater_nodes: &[Option<usize>]) -> &'static str {
    let Some(counter) = counter_node else {
        return "unknown";
    };
    if updater_nodes.is_empty() || updater_nodes.iter().any(Option::is_none) {
        return "unknown";
    }
    let first = updater_nodes[0];
    if updater_nodes.iter().any(|node| *node != first) {
        "mixed"
    } else if first == Some(counter) {
        "local"
    } else {
        "remote"
    }
}

/// The node a CPU belongs to, if known.
pub fn cpu_node(nodes: &[NodeCpus], cpu: usize) -> Option<usize> {
    nodes
        .iter()
        .find(|node| node.cpus.contains(&cpu))
        .map(|node| node.node)
}

/// The kernel's automatic NUMA balancing setting, which may migrate an
/// unbound counter's page toward the updaters during a run.
pub fn numa_balancing() -> Option<u32> {
    std::fs::read_to_string("/proc/sys/kernel/numa_balancing")
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// A value alone in its own page or pages, optionally bound to one NUMA node.
pub struct PlacedPage<T> {
    value: NonNull<T>,
    len: usize,
}

// SAFETY: `PlacedPage` owns its `T` exclusively, like `Box<T>`.
unsafe impl<T: Send> Send for PlacedPage<T> {}
// SAFETY: shared access only hands out `&T`.
unsafe impl<T: Sync> Sync for PlacedPage<T> {}

impl<T> PlacedPage<T> {
    /// Maps fresh pages, binds them to `node` if given, then writes `value`.
    pub fn new(value: T, node: Option<usize>) -> Result<Self, String> {
        let page = sys::page_size();
        let len = std::mem::size_of::<T>().max(1).div_ceil(page) * page;
        assert!(
            std::mem::align_of::<T>() <= page,
            "value alignment exceeds a page"
        );
        let raw = sys::map(len)?;
        if let Some(node) = node
            && let Err(err) = sys::bind(raw, len, node)
        {
            // SAFETY: `raw` was mapped above with `len` bytes and is unused.
            unsafe { sys::unmap(raw, len) };
            return Err(err);
        }
        let value_ptr = raw.cast::<T>();
        // SAFETY: the mapping is page-aligned, at least `size_of::<T>()` bytes,
        // writable, and not yet referenced. This is the page's first write.
        unsafe { value_ptr.as_ptr().write(value) };
        Ok(Self {
            value: value_ptr,
            len,
        })
    }

    /// Address of the value.
    pub fn address(&self) -> usize {
        self.value.as_ptr().addr()
    }

    /// The NUMA node that holds the value's page now, where the OS reports it.
    pub fn node(&self) -> Option<usize> {
        sys::page_node(self.value.as_ptr().cast())
    }
}

impl<T> Deref for PlacedPage<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: `value` was initialised in `new` and lives until `drop`.
        unsafe { self.value.as_ref() }
    }
}

impl<T> Drop for PlacedPage<T> {
    fn drop(&mut self) {
        // SAFETY: `value` is initialised and the mapping is ours; nothing
        // borrows it once `drop` runs.
        unsafe {
            std::ptr::drop_in_place(self.value.as_ptr());
            sys::unmap(self.value.cast(), self.len);
        }
    }
}

#[cfg(target_os = "linux")]
mod sys {
    use std::ptr::NonNull;

    const MPOL_BIND: libc::c_long = 2;
    const MPOL_MF_STRICT: libc::c_ulong = 1;
    const MPOL_MF_MOVE: libc::c_ulong = 2;
    const MPOL_F_NODE: libc::c_ulong = 1;
    const MPOL_F_ADDR: libc::c_ulong = 2;
    const NODE_MASK_WORDS: usize = 16;

    pub fn page_size() -> usize {
        // SAFETY: sysconf has no preconditions.
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        usize::try_from(size)
            .ok()
            .filter(|&size| size > 0)
            .unwrap_or(4096)
    }

    pub fn map(len: usize) -> Result<NonNull<u8>, String> {
        // SAFETY: an anonymous private mapping with no address hint.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(format!(
                "mmap of the counter page failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        NonNull::new(raw.cast()).ok_or_else(|| "mmap returned null".to_string())
    }

    /// # Safety
    /// `raw` and `len` must describe a mapping from `map` that is no longer used.
    pub unsafe fn unmap(raw: NonNull<u8>, len: usize) {
        // SAFETY: guaranteed by the caller.
        unsafe { libc::munmap(raw.as_ptr().cast(), len) };
    }

    pub fn bind(raw: NonNull<u8>, len: usize, node: usize) -> Result<(), String> {
        let bits = NODE_MASK_WORDS * libc::c_ulong::BITS as usize;
        if node >= bits {
            return Err(format!("NUMA node {node} is beyond the supported {bits}"));
        }
        let mut mask = [0 as libc::c_ulong; NODE_MASK_WORDS];
        let word_bits = libc::c_ulong::BITS as usize;
        mask[node / word_bits] |= 1 << (node % word_bits);
        // SAFETY: the range is our own page-aligned mapping and the mask holds
        // `bits` bits; the kernel reads one fewer than `maxnode`.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_mbind,
                raw.as_ptr(),
                len,
                MPOL_BIND,
                mask.as_ptr(),
                bits + 1,
                MPOL_MF_STRICT | MPOL_MF_MOVE,
            )
        };
        if rc != 0 {
            return Err(format!(
                "binding the counter page to NUMA node {node} failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    pub fn page_node(addr: *const u8) -> Option<usize> {
        let mut node: libc::c_int = -1;
        // SAFETY: with MPOL_F_NODE | MPOL_F_ADDR the kernel writes the node of
        // the page at `addr` into `node` and reads no node mask.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_get_mempolicy,
                &mut node as *mut libc::c_int,
                std::ptr::null_mut::<libc::c_ulong>(),
                0 as libc::c_ulong,
                addr,
                MPOL_F_NODE | MPOL_F_ADDR,
            )
        };
        (rc == 0).then(|| usize::try_from(node).ok()).flatten()
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use std::alloc::{Layout, alloc_zeroed, dealloc};
    use std::ptr::NonNull;

    pub fn page_size() -> usize {
        4096
    }

    fn layout(len: usize) -> Layout {
        Layout::from_size_align(len, page_size()).expect("page layout")
    }

    pub fn map(len: usize) -> Result<NonNull<u8>, String> {
        // SAFETY: `len` is a nonzero multiple of the page size.
        NonNull::new(unsafe { alloc_zeroed(layout(len)) })
            .ok_or_else(|| "allocating the counter page failed".to_string())
    }

    /// # Safety
    /// `raw` and `len` must describe an allocation from `map` that is no longer used.
    pub unsafe fn unmap(raw: NonNull<u8>, len: usize) {
        // SAFETY: guaranteed by the caller.
        unsafe { dealloc(raw.as_ptr(), layout(len)) };
    }

    pub fn bind(_raw: NonNull<u8>, _len: usize, node: usize) -> Result<(), String> {
        Err(format!(
            "placing the counter on NUMA node {node} needs Linux"
        ))
    }

    pub fn page_node(_addr: *const u8) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_parses_names_and_node_numbers() {
        assert_eq!("unplaced".parse(), Ok(Placement::Unplaced));
        assert_eq!("unbound".parse(), Ok(Placement::Unplaced));
        assert_eq!("Local".parse(), Ok(Placement::Local));
        assert_eq!("1".parse(), Ok(Placement::Node(1)));
        assert!("north".parse::<Placement>().is_err());
        assert_eq!(Placement::Node(3).to_string(), "node3");
    }

    #[test]
    fn cpu_lists_expand_ranges() {
        assert_eq!(
            parse_cpu_list("0-3,8,10-11\n"),
            Some(vec![0, 1, 2, 3, 8, 10, 11])
        );
        assert_eq!(parse_cpu_list(""), Some(vec![]));
        assert_eq!(parse_cpu_list("3-1"), None);
        let nodes = [
            NodeCpus {
                node: 0,
                cpus: vec![0, 1],
            },
            NodeCpus {
                node: 1,
                cpus: vec![2, 3],
            },
        ];
        assert_eq!(cpu_node(&nodes, 3), Some(1));
        assert_eq!(cpu_node(&nodes, 9), None);
    }

    #[test]
    fn relation_names_local_remote_mixed_and_unknown() {
        assert_eq!(relation(Some(0), &[Some(0), Some(0)]), "local");
        assert_eq!(relation(Some(1), &[Some(0)]), "remote");
        assert_eq!(relation(Some(0), &[Some(0), Some(1)]), "mixed");
        assert_eq!(relation(None, &[Some(0)]), "unknown");
        assert_eq!(relation(Some(0), &[None]), "unknown");
        assert_eq!(relation(Some(0), &[]), "unknown");
    }

    #[test]
    fn placed_values_own_page_aligned_memory_and_drop() {
        use std::sync::Arc;
        let marker = Arc::new(());
        let placed = PlacedPage::new((7u64, Arc::clone(&marker)), None).unwrap();
        assert_eq!(placed.0, 7);
        assert_eq!(placed.address() % 4096, 0);
        assert_eq!(Arc::strong_count(&marker), 2);
        drop(placed);
        assert_eq!(Arc::strong_count(&marker), 1);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn binding_to_a_cpus_node_puts_the_page_there() {
        let nodes = nodes();
        let Some(first) = nodes.first() else {
            return; // No NUMA topology exposed here.
        };
        let placed = PlacedPage::new(0u64, Some(first.node)).unwrap();
        assert_eq!(placed.node(), Some(first.node));
        if let Some(other) = nodes.get(1) {
            let placed = PlacedPage::new(0u64, Some(other.node)).unwrap();
            assert_eq!(placed.node(), Some(other.node));
        }
    }
}

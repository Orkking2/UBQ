use std::sync::atomic::AtomicU32;

/// Atomic storage for a packed pair of counters (`high` = claimed, `low` = committed).
pub type A = AtomicU32;

/// Packed `F::BITS`-bit counter representation manipulated through [`A`].
/// Layout: upper `H::BITS` bits = claimed count, lower `H::BITS` bits = committed count.
pub type F = u32;
/// Single `H::BITS`-bit half of a packed [`F`] counter.
pub type H = u16;

/// Number of element slots per block.
pub const L: H = 32;
const _: () = assert!(
    L != H::MAX,
    "L must != H::MAX; H::MAX is used as a sentinel"
);

/// Returns the lower `H::BITS` bits of a packed counter (committed count).
#[inline(always)]
pub const fn low(r: F) -> H {
    r as H
}

/// Returns the upper `H::BITS` bits of a packed counter (claimed count).
#[inline(always)]
pub const fn high(r: F) -> H {
    (r >> H::BITS) as H
}

/// Packs two [`H`] values into a [`F`]: `h` in the upper `H::BITS` bits, `l` in the lower.
#[inline(always)]
pub const fn merge(h: H, l: H) -> F {
    (h as F) << H::BITS | l as F
}

/// # [C1] STABILITY PREDICATE:
/// `stab(u) := high(u) == low(u) || low(u) == L`
/// Until stab(u) holds there are in-flight producers (resp. consumers):
/// slots in the range [low(u), high(u)) are allocated (resp. reserved)
/// but not yet written (resp. read). We must not claim any slot until the
/// block reaches a stable state. `backoff.snooze()` yields the thread to
/// give those producers (resp. consumers) time to commit (resp. consume).
#[inline(always)]
pub const fn stab(u: F) -> bool {
    high(u) == low(u) || low(u) == L
}
mod atomic_views;

fn main() {
    use atomic_views::AtomicInt;
    use portable_atomic::Ordering::Relaxed;

    let value = AtomicInt::new(41);
    value.as_u64().fetch_add(1, Relaxed);
    assert_eq!(value.as_u128().load(Relaxed), 42);
}

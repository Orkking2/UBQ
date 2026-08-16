pub fn new_filled_box_slice<T, F: Fn() -> T>(fill: F, cap: usize) -> Box<[T]> {
    std::iter::repeat_with(fill).take(cap).collect()
}

pub fn usize_as_u16_or_max(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

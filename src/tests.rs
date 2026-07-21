use std::{
    fmt::Debug,
    hint::black_box,
    println,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self},
    time::Instant,
    vec::Vec,
};

use crate::{ConfiguredUBQ, DEFAULT_BLOCK_SIZE, UBQ, align, backoff, ubq};

#[test]
fn drop_releases_all_enqueued_values() {
    let token = Arc::new(());
    let n = (DEFAULT_BLOCK_SIZE * 3) + 7;

    for _ in 0..16 {
        let q = UBQ::new();

        for _ in 0..n {
            q.push(token.clone());
        }

        assert_eq!(Arc::strong_count(&token), n + 1);

        println!("q: {q:?}");

        drop(q);

        assert_eq!(Arc::strong_count(&token), 1);
    }
}

#[test]
fn fill_drain_ordered() {
    let q = UBQ::new();

    let m = 1_000_000;
    for i in 0..m {
        q.push(i);
    }

    for i in 0..m {
        assert_eq!(q.pop(), Some(i));
    }
}

#[test]
fn refill_drain_recycled_blocks() {
    let q = UBQ::new();
    let per_round = DEFAULT_BLOCK_SIZE * 3 + 17;

    for round in 0..64 {
        for i in 0..per_round {
            q.push((round, i));
        }

        for i in 0..per_round {
            assert_eq!(q.pop(), Some((round, i)));
        }

        assert_eq!(q.pop(), None);
    }
}

#[test]
// 8x2x10_000_001
// Seg: 1.63769375s
// UBQ: 5.440279166s

// Notes:
// Look for page faults. VTune, perf
// Warm up before running tests.
// Look for better benchmarkers.
fn mpmc() {
    let q = UBQ::new_arc();
    // let q = Arc::new(SegQueue::new());

    let flag = Arc::new(AtomicBool::new(true));

    let epoch = Instant::now();

    let m = 1_000_001;
    let v: Vec<_> = (0..8)
        .map(|_| {
            (
                {
                    let q = q.clone();

                    thread::spawn(move || {
                        for i in 0..m {
                            q.push(black_box((i % u8::MAX as i32) as u8));
                        }
                    })
                },
                {
                    let flag = flag.clone();
                    let q = q.clone();

                    thread::spawn(move || {
                        for _ in 0..m {
                            loop {
                                if flag.load(Ordering::Acquire) {
                                    if black_box(q.pop()).is_some() {
                                        break;
                                    }
                                } else {
                                    assert!(black_box(q.pop()).is_some());
                                    break;
                                }
                            }
                        }
                    })
                },
            )
        })
        .collect();

    let v: Vec<_> = v
        .into_iter()
        .map(|(p, c)| {
            p.join().unwrap();
            c
        })
        .collect();

    flag.store(false, Ordering::Release);

    for c in v {
        c.join().unwrap()
    }

    println!("{:?}", epoch.elapsed());
}

#[test]
fn configured_queue_supports_non_default_pool_and_preset_block() {
    let q = ConfiguredUBQ::<u64, backoff::Crossbeam, 8, 127, align::A256>::new();

    for i in 0..10_000 {
        q.push(i);
    }

    for i in 0..10_000 {
        assert_eq!(q.pop(), Some(i));
    }

    assert_eq!(q.pop(), None);
}

#[test]
fn configured_queue_supports_arbitrary_block_with_explicit_alignment() {
    #[repr(align(1024))]
    #[derive(Clone, Copy, Debug, Default)]
    struct A1024;

    let q = ConfiguredUBQ::<u64, backoff::Crossbeam, 2, 100, A1024>::new();

    for i in 0..2_000 {
        q.push(i);
    }

    for i in 0..2_000 {
        assert_eq!(q.pop(), Some(i));
    }

    assert_eq!(q.pop(), None);
}

#[test]
fn ubq_macro_defaults_to_public_alias() {
    let q: ConfiguredUBQ<u64> = ubq!(type: u64);
    q.push(9);
    assert_eq!(q.pop(), Some(9));
}

#[test]
fn ubq_macro_applies_explicit_overrides() {
    let q: ConfiguredUBQ<u64, backoff::Yield, 2, 127, align::A256> = ubq!(
        type: u64,
        backoff: backoff::Yield,
        pool: 2,
        block: 127,
    );

    q.push(11);
    assert_eq!(q.pop(), Some(11));
}

#[test]
fn ubq_macro_supports_custom_alignment_override() {
    #[repr(align(1024))]
    #[derive(Clone, Copy, Debug, Default)]
    struct A1024;

    let q: ConfiguredUBQ<u64, backoff::Crossbeam, 4, 100, A1024> = ubq!(
        type: u64,
        pool: 4,
        block: 100,
        align: A1024,
    );

    q.push(13);
    assert_eq!(q.pop(), Some(13));
}

// Seg: 2.12s
// UBQ: 5.15s
#[test]
fn push_test() {
    let q = UBQ::new_arc();
    // let q = Arc::new(SegQueue::new());

    let epoch = Instant::now();

    let v = (0..8)
        .map(|_| {
            let q = q.clone();

            thread::spawn(move || {
                for i in 0..1_000_000 {
                    q.push(black_box(i));
                }
            })
        })
        .collect::<Vec<_>>();

    v.into_iter().for_each(|h| h.join().unwrap());

    println!("{:?}", epoch.elapsed());
}

// #[test]
// fn is_empty_returns_correctly() {
//     assert!(UBQ::<()>::new().is_empty());

//     for m in 1_000..1_005 {
//         let q = UBQ::new();

//         for i in 0..m {
//             q.push(i);
//         }

//         for _ in 0..m {
//             q.pop().unwrap();
//         }

//         assert!(q.is_empty())
//     }
// }

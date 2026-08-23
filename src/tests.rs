use std::{
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

use crate::{UBQ, backoff};

#[test]
fn block_length_is_derived_from_one_page() {
    let queue = UBQ::<usize>::new();
    let block_length = queue.block_length();
    assert!(block_length > 0);

    let count = block_length + 3;
    queue.push_batch(0..count);
    assert_eq!(
        queue.pop_batch(count).collect::<Vec<_>>(),
        (0..count).collect::<Vec<_>>()
    );
}

#[test]
fn drop_releases_all_enqueued_values() {
    let token = Arc::new(());
    let queue = UBQ::<Arc<()>>::new();
    let n = (queue.block_length() * 3) + 7;
    drop(queue);

    for _ in 0..16 {
        let q = UBQ::<Arc<()>>::new();

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
    let q = UBQ::<i32>::new();

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
    let q = UBQ::<(usize, usize)>::new();
    let per_round = q.block_length() * 3 + 17;

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
    let q = UBQ::<u8>::new_arc();
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
fn default_queue_uses_public_ubq_type() {
    let q = UBQ::<u64>::new();
    q.push(9);
    assert_eq!(q.pop(), Some(9));
}

#[test]
fn queue_accepts_explicit_backoff() {
    let q = UBQ::<u64, backoff::Yield>::new();

    q.push(11);
    assert_eq!(q.pop(), Some(11));
}

#[test]
fn queue_arc_constructor_uses_explicit_configuration() {
    let q = UBQ::<u64, backoff::Crossbeam>::new_arc();

    q.push(13);
    assert_eq!(q.pop(), Some(13));
}

// Seg: 2.12s
// UBQ: 5.15s
#[test]
fn push_test() {
    let q = UBQ::<i32>::new_arc();
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

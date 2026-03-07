use std::{
    fmt::Debug,
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self},
    time::Instant,
    usize,
};

use crate::{BLOCK_LENGTH, UBQ};

impl<T> Debug for UBQ<T> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
        // let p = self.p.load(Ordering::Acquire);

        // if p.is_null() {
        //     return writeln!(f, "UBQ {{}}");
        // }

        // let mut s = String::new();
        // let mut c = p;

        // let fmt = |u: R| -> String {
        //     if u == R::MAX {
        //         format!("full:full")
        //     } else {
        //         format!("{:04}:{:04}", high(u), low(u))
        //     }
        // };

        // loop {
        //     let p_ = unsafe { *(*c).p.as_ptr() };
        //     let c_ = unsafe { *(*c).c.as_ptr() };

        //     write!(s, "\t{c:p}: p={}, c={}", fmt(p_), fmt(c_))?;

        //     c = unsafe { *(*c).n.as_ptr() };
        //     if c == p {
        //         break;
        //     }

        //     write!(s, "\n")?;
        // }

        // write!(f, "UBQ {{\n{s}\t}}")
    }
}

#[test]
fn drop_releases_all_enqueued_values() {
    let token = Arc::new(());
    let n = (BLOCK_LENGTH as usize * 3) + 7;

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
    let per_round = BLOCK_LENGTH * 3 + 17;

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

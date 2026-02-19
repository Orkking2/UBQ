use std::{
    fmt::{Debug, Write},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    usize,
};

use crate::{
    L, UBQ,
    packed::{F, high, low},
};

impl<T> Debug for UBQ<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = unsafe { self.i.as_ref().p.load(Ordering::Acquire) };

        if p.is_null() {
            return writeln!(f, "UBQ {{}}");
        }

        let mut s = String::new();
        let mut c = p;

        let fmt = |u: F| -> String {
            if u == F::MAX {
                format!("full:full")
            } else {
                format!("{:04}:{:04}", high(u), low(u))
            }
        };

        loop {
            let p_ = unsafe { *(*c).p.as_ptr() };
            let c_ = unsafe { *(*c).c.as_ptr() };

            write!(s, "\t{c:p}: p={}, c={}", fmt(p_), fmt(c_))?;

            c = unsafe { *(*c).n.as_ptr() };
            if c == p {
                break;
            }

            write!(s, "\n")?;
        }

        write!(f, "UBQ {{\n{s}\t}}")
    }
}

struct DropProbe {
    dropped: Arc<AtomicUsize>,
}

impl DropProbe {
    fn new(dropped: Arc<AtomicUsize>) -> Self {
        Self { dropped }
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn drop_releases_all_enqueued_values() {
    let token = Arc::new(());
    let n = (L as usize * 3) + 7;

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
fn drop_of_final_clone_drops_items_left_in_queue() {
    let dropped = Arc::new(AtomicUsize::new(0));

    let q = UBQ::new();
    let q1 = q.clone();
    let q2 = q.clone();

    let total = (L as usize * 2) + 5;
    let popped = (L as usize / 2) + 1;

    for _ in 0..(L as usize + 1) {
        q.push(DropProbe::new(dropped.clone()));
    }
    for _ in 0..(total - (L as usize + 1)) {
        q1.push(DropProbe::new(dropped.clone()));
    }

    let mut held = Vec::with_capacity(popped);
    for _ in 0..popped {
        held.push(
            q2.pop()
                .expect("queue should contain enough elements for this test"),
        );
    }

    drop(q);
    drop(q1);

    assert_eq!(dropped.load(Ordering::SeqCst), 0);

    let remaining = total - popped;
    drop(q2);

    assert_eq!(dropped.load(Ordering::SeqCst), remaining);

    drop(held);

    assert_eq!(dropped.load(Ordering::SeqCst), total);
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
fn mpmc_4p4c() {
    let q = UBQ::new();

    let flag = Arc::new(AtomicBool::new(true));

    let pf = |q: UBQ<usize>, m: usize| -> JoinHandle<()> {
        thread::spawn(move || {
            for i in 0..m {
                q.push(i);
            }
        })
    };

    let cf = |q: UBQ<usize>, m: usize| -> JoinHandle<()> {
        let flag = flag.clone();

        thread::spawn(move || {
            for _ in 0..m {
                loop {
                    if flag.load(Ordering::Acquire) {
                        if q.pop().is_some() {
                            break;
                        }
                    } else {
                        assert!(q.pop().is_some());
                        break;
                    }
                }
            }
        })
    };

    let m = 1_000_001;
    let v: Vec<_> = (0..4)
        .map(|_| (pf(q.clone(), m), cf(q.clone(), m)))
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
}

#[test]
fn is_empty_returns_correctly() {
    assert!(UBQ::<()>::new().is_empty());

    for m in 1_000..1_005 {
        let q = UBQ::new();

        for i in 0..m {
            q.push(i);
        }

        for _ in 0..m {
            q.pop().unwrap();
        }

        assert!(q.is_empty())
    }
}

#[test]
fn shrink_removes_all_in_empty_queue() {
    for m in 1_000..1_005 {
        let q = UBQ::new();

        for i in 0..m {
            q.push(i);
        }

        for _ in 0..m {
            q.pop().unwrap();
        }

        assert!(q.is_empty());

        // SAFETY: No in-flight push's or pop's
        unsafe { q.shrink() };

        assert_eq!(unsafe { *q.i.as_ref().p.as_ptr() }, ptr::null_mut())
    }
}

use crate::block::{Block, ReserveError};
use atomic_list::{cursor::Cursor, sync::Node};
// #[cfg(test)]
// use std::{thread::sleep, time::Duration};
use std::{
    iter,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct UBQ<T> {
    phead: Cursor<Block<T>>,
    chead: Cursor<Block<T>>,

    cache: Option<Block<T>>,

    max_access: NonNull<AtomicUsize>,
    currently_allocated: NonNull<AtomicUsize>,

    bounds: NonNull<UBQBounds>,
    default_block_size: usize,
}

unsafe impl<T> Sync for UBQ<T> {}
unsafe impl<T> Send for UBQ<T> {}

impl<T> Clone for UBQ<T> {
    fn clone(&self) -> Self {
        let Self {
            cache: _,

            phead,
            chead,

            max_access,
            currently_allocated,

            bounds,
            default_block_size,
        } = self;

        let new_max_access = self.max_access().fetch_add(1, Ordering::Relaxed) + 1;

        Cursor::get_current(&phead).unique_iter().for_each(|node| {
            node.maybe_double_max_access(new_max_access);
        });

        Self {
            cache: None,

            currently_allocated: currently_allocated.clone(),
            default_block_size: default_block_size.clone(),
            max_access: max_access.clone(),
            bounds: bounds.clone(),
            phead: phead.clone(),
            chead: chead.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UBQBounds {
    pub max: usize,
    pub min: usize,
}

impl<T> UBQ<T> {
    pub fn new(default_block_size: usize, bounds: UBQBounds) -> Self {
        #[cfg(test)]
        println!("new with default_blk_sz: {default_block_size} bounds: {bounds:?}");

        debug_assert!(
            bounds.min >= 1,
            "we maintain the invariant that every UBQ is comprised of at least one block"
        );
        let mut init_node = Node::new(Block::new(default_block_size));

        init_node.extend(
            iter::from_fn(|| Some(Block::new(default_block_size))).take((bounds.min - 1) as usize),
        );

        let max_access =
            unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(1)))) };

        let currently_allocated = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(bounds.min))))
        };

        let bounds = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(bounds))) };

        Self {
            phead: Cursor::new(init_node.clone()),
            chead: Cursor::new(init_node),
            cache: None,

            currently_allocated,
            default_block_size,
            max_access,
            bounds,
        }
    }

    fn max_access(&self) -> &AtomicUsize {
        unsafe { self.max_access.as_ref() }
    }

    fn load_max_access(&self) -> usize {
        self.max_access().load(Ordering::Relaxed)
    }

    pub fn allocate_new_block(&mut self, block_size: Option<usize>) -> bool {
        match self.phead.push_before(
            self.cache.take().unwrap_or(Block::new(
                block_size.unwrap_or(self.default_block_size) + self.load_max_access(),
            )),
            Block::is_focus_of_consumers,
        ) {
            Ok(()) => true,
            Err(block) => {
                self.cache.replace(block);
                false
            }
        }
    }

    fn currently_allocated(&self) -> &AtomicUsize {
        unsafe { self.currently_allocated.as_ref() }
    }

    fn bounds(&self) -> UBQBounds {
        *unsafe { self.bounds.as_ref() }
    }

    pub fn push(&mut self, val: T) -> Result<(), (T, PushErr)> {
        let mut attempts_at_capacity_limit = 0usize;

        loop {
            #[cfg(test)]
            println!("push loop start (len={})", self.phead.len());
            if let Some(res) = self.phead.allocate() {
                #[cfg(test)]
                println!("allocated slot");
                break Ok(res.write(val));
            } else {
                #[cfg(test)]
                println!("allocation returned None");
                // If the next block is not the focus of consumers (e.g. it is empty),
                // we simply call next on phead.
                //
                // If we are within bounds to allocate a new block we will try to do so.
                // If we are not within bounds, we fail and return the element.
                if let Some(peeked) = self.phead.peek() {
                    #[cfg(test)]
                    println!(
                        "peeked block, focus_of_consumers={}",
                        peeked.is_focus_of_consumers()
                    );
                    let at_block_limit =
                        self.currently_allocated().load(Ordering::Relaxed) >= self.bounds().max;

                    if at_block_limit || !peeked.is_focus_of_consumers() {
                        #[cfg(test)]
                        println!("incrementing phead");
                        Cursor::increment(&mut self.phead);
                        self.phead.reset_pcon();

                        if at_block_limit {
                            #[cfg(test)]
                            println!("at block limit");

                            attempts_at_capacity_limit += 1;

                            if attempts_at_capacity_limit
                                >= self.currently_allocated().load(Ordering::Relaxed)
                            {
                                #[cfg(test)]
                                println!("break with error");
                                break Err((val, PushErr::BlockAllocBoundsReached));
                            }
                        } else {
                            attempts_at_capacity_limit = 0;
                        }

                        continue;
                    }

                    if peeked.is_focus_of_consumers() {
                        let curr_alloc = self.currently_allocated();

                        match curr_alloc.fetch_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |currently_allocated| {
                                currently_allocated
                                    .checked_add(1)
                                    .filter(|&ca| ca <= self.bounds().max)
                            },
                        ) {
                            Ok(_) => {
                                match self.phead.push_before(
                                    self.cache
                                        .take()
                                        .unwrap_or_else(|| Block::new(self.default_block_size)),
                                    Block::is_focus_of_consumers,
                                ) {
                                    Ok(()) => {
                                        attempts_at_capacity_limit = 0;
                                        continue;
                                    }
                                    Err(block) => {
                                        self.cache.replace(block);
                                    }
                                }
                            }
                            Err(_) => {
                                #[cfg(test)]
                                println!("incrementing phead after failed allocation");
                                Cursor::increment(&mut self.phead);
                                self.phead.reset_pcon();

                                if at_block_limit {
                                    attempts_at_capacity_limit += 1;

                                    if attempts_at_capacity_limit
                                        >= self.currently_allocated().load(Ordering::Relaxed)
                                    {
                                        break Err((val, PushErr::BlockAllocBoundsReached));
                                    }
                                } else {
                                    attempts_at_capacity_limit = 0;
                                }

                                continue;
                            }
                        }
                    }
                } else {
                    #[cfg(test)]
                    println!("peeked None");
                    break Err((val, PushErr::ListHasBeenDeallocated));
                }
            }
        }
    }

    pub fn pop(&mut self) -> Result<T, PopErr> {
        loop {
            // #[cfg(test)]
            // println!("Starting pop loop");
            // #[cfg(test)]
            // sleep(Duration::from_millis(500));

            match self.chead.reserve() {
                Ok(res) => break Ok(res.read()),

                Err(ReserveError::NoEntry) => break Err(PopErr::Empty),
                Err(ReserveError::NotAvailable) => break Err(PopErr::Busy),

                Err(ReserveError::BlockDone) => {}
            }

            Cursor::increment(&mut self.chead);
            // #[cfg(test)]
            // println!("Incremented chead");
            self.chead.reset_ccon();
        }
    }
}

#[derive(Debug)]
pub enum PopErr {
    Empty,
    Busy,
}

#[derive(Debug)]
pub enum PushErr {
    BlockAllocBoundsReached,
    ListHasBeenDeallocated,
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        let new_max_access = self.max_access().fetch_sub(1, Ordering::Relaxed) - 1;

        Cursor::get_current(&self.phead)
            .unique_iter()
            .for_each(|node| {
                node.maybe_halve_max_access(new_max_access);
            });

        if new_max_access == 0 {
            drop(unsafe { Box::from_raw(self.currently_allocated.as_ptr()) });
            drop(unsafe { Box::from_raw(self.max_access.as_ptr()) });
            drop(unsafe { Box::from_raw(self.bounds.as_ptr()) });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_queue(block_size: usize, min: usize, max: usize) -> UBQ<i32> {
        UBQ::new(block_size, UBQBounds { min, max })
    }

    #[test]
    fn push_pop_within_single_block() {
        let mut q = new_queue(2, 2, 2);

        println!("created queue");

        for i in 0..3 {
            println!("pushing {i}");
            let res = q.push(i);
            assert!(res.is_ok(), "push {i} failed: {:?}", res);
        }

        for i in 0..3 {
            println!("popping {i}");
            assert_eq!(q.pop().unwrap(), i);
        }

        assert!(matches!(q.pop(), Err(PopErr::Empty)));
    }

    #[test]
    fn allocates_new_block_until_max_bound() {
        let mut q = new_queue(2, 2, 3);

        let results = (1..=6).map(|v| {
            println!("push {v}");
            (v, q.push(v))
        });
        for (value, res) in results {
            assert!(res.is_ok(), "push {value} failed: {:?}", res);
        }

        println!("push seventh");
        let seventh = q.push(7);
        assert!(matches!(
            seventh,
            Err((7, PushErr::BlockAllocBoundsReached))
        ));

        for expected in 1..=6 {
            println!("popping {expected}");
            assert_eq!(q.pop().unwrap(), expected);
        }
        println!("popping seventh");
        assert!(matches!(q.pop(), Err(PopErr::Empty)));
    }
}

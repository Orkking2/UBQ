use crate::block::{Block, ReserveError};
use crossbeam_utils::Backoff;
use atomic_list::{cursor::Cursor, sync::Node};
use std::{
    iter,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "ubq_debug")]
use crate::block::BlockDebug;

pub struct UBQ<T> {
    phead: Cursor<Block<T>>,
    chead: Cursor<Block<T>>,

    cache: Option<Block<T>>,

    max_access: NonNull<AtomicUsize>,
    currently_allocated: NonNull<AtomicUsize>,
    len: NonNull<AtomicUsize>,

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
            len,

            bounds,
            default_block_size,
        } = self;

        let new_max_access = self.max_access().fetch_add(1, Ordering::Relaxed) + 1;

        Cursor::get_current(&phead).unique_iter().for_each(|node| {
            node.set_max_access(new_max_access);
        });

        Self {
            cache: None,

            currently_allocated: currently_allocated.clone(),
            default_block_size: default_block_size.clone(),
            max_access: max_access.clone(),
            len: len.clone(),
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

#[cfg(feature = "ubq_debug")]
pub struct QueueState<T> {
    pub pnode: Node<Block<T>>,
    pub phead: BlockDebug<T>,
    pub chead: BlockDebug<T>,
    pub allocated_blocks: usize,
}

// impl<T> Drop for QueueState<T> {
//     fn drop(&mut self) {
//         let QueueState {
//             // phead_block,
//             // chead_block,
//             ..
//         } = self;

//         drop(unsafe { Node::from_raw(phead_block) });
//         drop(unsafe { Node::from_raw(chead_block) });
//     }
// }

#[cfg(feature = "ubq_debug")]
impl<T> core::fmt::Debug for QueueState<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "QueueState: {{\n")?;
        write!(f, "\talloc'ed: {:?},\n", &self.allocated_blocks)?;
        write!(f, "\tphead: {:?},\n", &self.phead)?;
        write!(f, "\tchead: {:?},\n", &self.chead)?;
        write!(f, "\tblocks: [\n")?;

        for debug_state in self.pnode.unique_iter().map(|n| Block::debug(&n)) {
            write!(f, "\t\t{debug_state:?},\n")?;
        }

        write!(f, "\t]\n}}")?;

        Ok(())

        // f.debug_struct("QueueState")
        //     .field("phead", &self.phead)
        //     .field("chead", &self.chead)
        //     .field("allocated_blocks", &self.allocated_blocks)
        //     .field(
        //         "blocks",
        //         &Vec::from_iter(),
        //     )
        //     .finish()
    }
}

impl<T> UBQ<T> {
    pub fn new(default_block_size: usize, bounds: UBQBounds) -> Self {
        #[cfg(feature = "ubq_debug")]
        log::trace!(
            "initializing UBQ default_block_size={} bounds={bounds:?}",
            default_block_size
        );

        debug_assert!(
            bounds.min >= 1,
            "we maintain the invariant that every UBQ is comprised of at least one block"
        );
        let init_node = Node::new(Block::new(default_block_size, 1));

        for block in iter::from_fn(|| Some(Block::new(default_block_size, 1)))
            .take((bounds.min - 1) as usize)
        {
            if init_node.push_before(block, |_| true).is_err() {
                panic!("push_before should always succeed with `predicate: |_| true`");
            }
        }

        #[cfg(feature = "ubq_debug")]
        log::trace!("Blocks are as follows: [\n{}]", {
            let mut s = String::new();

            for node in init_node.unique_iter() {
                s += &format!("\t{:?},\n", Block::debug(&node));
            }

            s
        });

        let max_access =
            unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(1)))) };

        let currently_allocated = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(bounds.min))))
        };

        let len = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(0)))) };

        let bounds = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(bounds))) };

        Self {
            phead: Cursor::new(init_node.clone()),
            chead: Cursor::new(init_node),
            cache: None,

            currently_allocated,
            default_block_size,
            max_access,
            len,
            bounds,
        }
    }

    fn max_access(&self) -> &AtomicUsize {
        unsafe { self.max_access.as_ref() }
    }

    fn load_max_access(&self) -> usize {
        self.max_access().load(Ordering::Relaxed)
    }

    pub fn allocated_blocks(&self) -> usize {
        self.currently_allocated()
            .load(Ordering::Relaxed)
    }

    pub fn allocate_new_block(&mut self, block_size: Option<usize>) -> bool {
        match self.phead.push_before(
            self.cache.take().unwrap_or_else(|| {
                Block::new(
                    block_size.unwrap_or(self.default_block_size),
                    self.load_max_access(),
                )
            }),
            Block::not_available_for_producing,
        ) {
            Ok(_) => {
                let _curr = Cursor::get_current(&self.phead).clone();
                // log::trace!(
                //     "allocate_new_block: inserted before phead current={:p} allocated_blocks={}",
                //     (&*curr) as *const Block<T>,
                //     self.allocated_blocks()
                // );
                true
            }
            Err(block) => {
                self.cache.replace(block);
                false
            }
        }
    }

    fn currently_allocated(&self) -> &AtomicUsize {
        unsafe { self.currently_allocated.as_ref() }
    }

    fn len(&self) -> &AtomicUsize {
        unsafe { self.len.as_ref() }
    }

    fn bounds(&self) -> UBQBounds {
        *unsafe { self.bounds.as_ref() }
    }

    #[cfg(feature = "ubq_debug")]
    pub fn debug_state(&mut self) -> QueueState<T> {
        let pnode = Cursor::get_current(self.phead.reload()).clone();
        let cnode = Cursor::get_current(self.chead.reload()).clone();

        QueueState {
            phead: Block::debug(&pnode),
            chead: Block::debug(&cnode),
            allocated_blocks: self.allocated_blocks(),
            pnode: pnode.clone(),
        }
    }

    #[cfg(feature = "ubq_debug")]
    fn incr_phead(&mut self, debug_block: BlockDebug<T>) {
        let mut prepared_debug_block = None;
        let mut did_reset_pcon = false;
        let did_increment_cursor = Cursor::increment_with(&mut self.phead, |next| {
            did_reset_pcon = next.reset_pcon();
            prepared_debug_block = Some(Block::debug(next));
        });
        let incremented_debug_block = Block::debug(Cursor::get_current(&self.phead));


        log::trace!(
            "Increment phead ({did_increment_cursor}) {debug_block:?} -> {incremented_debug_block:?}",
        );

        log::trace!(
            "self.phead.reset_pcon() ({did_reset_pcon}) called on {prepared_debug_block:?}, resulting in {:?}",
            Block::debug(Cursor::get_current(&self.phead)),
        );
    }

    #[cfg(not(feature = "ubq_debug"))]
    fn incr_phead(&mut self) {
        let _ = Cursor::increment_with(&mut self.phead, |next| {
            let _ = next.reset_pcon();
        });
    }

    #[cfg(feature = "ubq_debug")]
    fn incr_chead(&mut self, debug_block: BlockDebug<T>) {
        let mut prepared_debug_block = None;
        let mut did_reset_ccon = false;
        let did_increment_cursor = Cursor::increment_with(&mut self.chead, |next| {
            did_reset_ccon = next.reset_ccon();
            prepared_debug_block = Some(Block::debug(next));
        });
        let incremented_debug_block = Block::debug(Cursor::get_current(&self.chead));

        log::trace!(
            "Increment chead ({did_increment_cursor}) {debug_block:?} -> {incremented_debug_block:?}",
        );

        log::trace!(
            "self.chead.reset_ccon() ({did_reset_ccon}) called on {prepared_debug_block:?}, resulting in {:?}",
            Block::debug(Cursor::get_current(&self.chead)),
        );
    }

    #[cfg(not(feature = "ubq_debug"))]
    fn incr_chead(&mut self) {
        let _ = Cursor::increment_with(&mut self.chead, |next| {
            let _ = next.reset_ccon();
        });
    }

    pub fn push(&mut self, val: T) -> Result<(), (T, PushErr)> {
        loop {
            self.phead.reload();

            #[cfg(feature = "ubq_debug")]
            let debug_block = Block::debug(Cursor::get_current(&self.phead));

            if let Some(reservation) = self.phead.allocate() {
                #[cfg(feature = "ubq_debug")]
                log::trace!(
                    "self.phead.allocate() returns Some({{i: {}}}) ({debug_block:?})",
                    reservation.get_idx(),
                );

                reservation.write(val);
                self.len().fetch_add(1, Ordering::Release);
                break Ok(());
            } else {
                #[cfg(feature = "ubq_debug")]
                log::warn!("self.phead.allocate() returns None ({debug_block:?})");
                match self
                    .phead
                    .peek()
                    .map(|peeked| peeked.available_for_producing())
                {
                    Some(true) => {
                        #[cfg(feature = "ubq_debug")]
                        log::trace!(
                            "peeked.available_for_producing() is true ({debug_block:?})",
                        );

                        #[cfg(feature = "ubq_debug")]
                        self.incr_phead(debug_block);
                        #[cfg(not(feature = "ubq_debug"))]
                        self.incr_phead();
                    }
                    Some(false) => {
                        #[cfg(feature = "ubq_debug")]
                        log::trace!(
                            "peeked.available_for_producing() is false ({debug_block:?})",
                        );
                        match self.currently_allocated().fetch_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |ca| ca.checked_add(1).filter(|&ca| ca <= self.bounds().max),
                        ) {
                            #[allow(unused_variables)]
                            Ok(x) => {
                                #[cfg(feature = "ubq_debug")]
                                log::trace!(
                                    "self.currently_allocated().fetch_update() returns Ok({x})"
                                );
                                match self.phead.push_before_peeked(
                                    self.cache.take().unwrap_or_else(|| {
                                        Block::new(self.default_block_size, self.load_max_access())
                                    }),
                                    Block::not_available_for_producing,
                                ) {
                                    #[allow(unused_variables)]
                                    Ok(node) => {
                                        #[cfg(feature = "ubq_debug")]
                                        log::trace!(
                                            "self.phead.push_before_peeked() returns Ok({:?}) ({debug_block:?})",
                                            Block::debug(&node)
                                        );
                                        // Start producing into the newly inserted block instead of
                                        // spinning on the full one we just left.
                                        #[cfg(feature = "ubq_debug")]
                                        self.incr_phead(debug_block);
                                        #[cfg(not(feature = "ubq_debug"))]
                                        self.incr_phead();
                                        continue;
                                    }
                                    Err(block) => {
                                        #[cfg(feature = "ubq_debug")]
                                        log::warn!(
                                            "self.phead.push_before_peeked() returns Err(block) ({debug_block:?})",
                                        );
                                        self.cache.replace(block);
                                    }
                                }
                            }
                            #[allow(unused_variables)]
                            Err(x) => {
                                #[cfg(feature = "ubq_debug")]
                                log::error!(
                                    "self.currently_allocated().fetch_update() returns Err({x}) ({debug_block:?})",
                                );
                                break Err((val, PushErr::BlockAllocBoundsReached));
                            }
                        }
                    }
                    None => {
                        #[cfg(feature = "ubq_debug")]
                        log::error!("self.phead.peek() is None ({debug_block:?})",);
                        break Err((val, PushErr::ListHasBeenDeallocated));
                    }
                }
            }
        }
    }

    pub fn pop(&mut self) -> Result<T, PopErr> {
        loop {
            self.chead.reload();

            #[cfg(feature = "ubq_debug")]
            let debug_block = Block::debug(Cursor::get_current(&self.chead));

            let err = match self.chead.reserve() {
                Ok(res) => {
                    #[cfg(feature = "ubq_debug")]
                    log::trace!("self.chead.reserve() returns Ok(res) ({debug_block:?})");
                    let v = res.read();
                    self.len().fetch_sub(1, Ordering::AcqRel);
                    break Ok(v);
                }
                Err(err) => err,
            };

            match err {
                ReserveError::NoEntry => {
                    #[cfg(feature = "ubq_debug")]
                    log::warn!("self.chead.reserve() returns Err(NoEntry) ({debug_block:?})");

                    let has_items = self.len().load(Ordering::Acquire) > 0;
                    let is_brand_new = Cursor::get_current(&self.chead).is_brand_new();

                    if has_items && is_brand_new {
                        #[cfg(feature = "ubq_debug")]
                        log::warn!("skipping brand-new block while items remain in queue");
                        #[cfg(feature = "ubq_debug")]
                        self.incr_chead(debug_block);
                        #[cfg(not(feature = "ubq_debug"))]
                        self.incr_chead();
                        continue;
                    }

                    break Err(PopErr::Empty);
                }
                ReserveError::NotAvailable => {
                    #[cfg(feature = "ubq_debug")]
                    log::warn!("self.chead.reserve() returns Err(NotAvailable) ({debug_block:?})");
                    break Err(PopErr::Busy);
                }
                ReserveError::BlockDone => {
                    #[cfg(feature = "ubq_debug")]
                    log::warn!("self.chead.reserve() returns Err(BlockDone) ({debug_block:?})");
                }
            }

            #[cfg(feature = "ubq_debug")]
            self.incr_chead(debug_block);
            #[cfg(not(feature = "ubq_debug"))]
            self.incr_chead();
        }
    }

    /// Spin + yield until a push succeeds or the list is deallocated.
    pub fn push_spin(&mut self, mut val: T) -> Result<(), PushErr> {
        let mut backoff = Backoff::new();
        loop {
            match self.push(val) {
                Ok(()) => return Ok(()),
                Err((v, PushErr::BlockAllocBoundsReached)) => {
                    val = v;
                    backoff.snooze();
                }
                Err((_v, PushErr::ListHasBeenDeallocated)) => {
                    return Err(PushErr::ListHasBeenDeallocated);
                }
            }
        }
    }

    /// Spin + yield until a pop succeeds.
    pub fn pop_spin(&mut self) -> Result<T, PopErr> {
        let mut backoff = Backoff::new();
        loop {
            match self.pop() {
                Ok(v) => return Ok(v),
                Err(PopErr::Empty) | Err(PopErr::Busy) => backoff.snooze(),
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PopErr {
    Empty,
    Busy,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushErr {
    BlockAllocBoundsReached,
    ListHasBeenDeallocated,
}

impl<T> Drop for UBQ<T> {
    fn drop(&mut self) {
        let new_max_access = self.max_access().fetch_sub(1, Ordering::Relaxed) - 1;

        // Cursor::get_current(&self.phead)
        //     .unique_iter()
        //     .for_each(|node| {
        //         node.set_max_access(new_max_access);
        //     });

        if new_max_access == 0 {
            drop(unsafe { Box::from_raw(self.currently_allocated.as_ptr()) });
            drop(unsafe { Box::from_raw(self.max_access.as_ptr()) });
            drop(unsafe { Box::from_raw(self.len.as_ptr()) });
            drop(unsafe { Box::from_raw(self.bounds.as_ptr()) });
        }
    }
}

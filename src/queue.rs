// Copyright 2024 ihciah. All Rights Reserved.

use std::{
    collections::VecDeque,
    future::Future,
    hint::spin_loop,
    io,
    mem::{self, MaybeUninit},
    os::fd::RawFd,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    sync::Arc,
    task::Waker,
};

use parking_lot::Mutex;
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tokio::{select, spawn, task::yield_now};

use crate::eventfd::{new_pair, Awaiter, Notifier};

pub struct Guard {
    _rx: Receiver<()>,
}

pub struct ReadQueue<T> {
    queue: Queue<T>,
    unstuck_notifier: Notifier,
    tokio_handle: Option<tokio::runtime::Handle>,
}

impl<T> ReadQueue<T> {
    #[inline]
    pub fn meta(&self) -> QueueMeta {
        self.queue.meta()
    }

    fn log_read_state(reason: &str, queue: &Queue<T>) {
        // println!(
        //     "[rust][read][{reason}] queue_len={} working={} stuck={} full={} empty={}",
        //     queue.len(),
        //     queue.working(),
        //     queue.stuck(),
        //     queue.is_full(),
        //     queue.is_empty()
        // );
    }

    pub fn pop(&mut self) -> Option<T> {
        let maybe_item = self.queue.pop();
        if self.queue.stuck() {
            self.queue.mark_unstuck();
            self.unstuck_notifier.notify().ok();
        }
        maybe_item
    }

    pub fn run_handler(self, handler: impl FnMut(T) + Send + 'static) -> Result<Guard, io::Error>
    where
        T: Send + 'static,
    {
        let working_awaiter = unsafe { Awaiter::from_raw_fd(self.queue.working_fd)? };
        let (tx, rx) = channel();
        if let Some(tokio_handle) = self.tokio_handle.clone() {
            tokio_handle.spawn(self.working_handler(working_awaiter, handler, tx));
        } else {
            spawn(self.working_handler(working_awaiter, handler, tx));
        }
        Ok(Guard { _rx: rx })
    }

    async fn working_handler(
        mut self,
        mut working_awaiter: Awaiter,
        mut handler: impl FnMut(T) + Send,
        mut tx: Sender<()>,
    ) where
        T: Send,
    {
        const WARM_SPIN_LIMIT: usize = 1024;
        const WARM_SPIN_YIELD_INTERVAL: usize = 64;
        let mut exit = std::pin::pin!(tx.closed());
        self.queue.mark_working();

        'p: loop {
            while let Some(item) = self.pop() {
                handler(item);
            }

            let mut spin_budget = WARM_SPIN_LIMIT;
            let mut yield_budget = WARM_SPIN_YIELD_INTERVAL;
            while spin_budget > 0 {
                if let Some(item) = self.pop() {
                    handler(item);
                    continue 'p;
                }
                if !self.queue.is_empty() {
                    spin_budget = WARM_SPIN_LIMIT;
                    yield_budget = WARM_SPIN_YIELD_INTERVAL;
                    continue;
                }

                spin_budget -= 1;
                if yield_budget == 0 {
                    yield_now().await;
                    yield_budget = WARM_SPIN_YIELD_INTERVAL;
                } else {
                    yield_budget -= 1;
                    spin_loop();
                }
            }

            if !self.queue.mark_unworking() {
                Self::log_read_state("continue-active", &self.queue);
                continue;
            }

            Self::log_read_state("before-wait", &self.queue);
            select! {
                _ = working_awaiter.wait() => (),
                _ = &mut exit => {
                    Self::log_read_state("exit", &self.queue);
                    return;
                }
            }
            self.queue.mark_working();
            Self::log_read_state("after-wake", &self.queue);
        }
    }
}

pub struct WriteQueue<T> {
    inner: Arc<Mutex<WriteQueueInner<T>>>,
    working_notifier: Arc<Notifier>,
}

impl<T> Clone for WriteQueue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            working_notifier: self.working_notifier.clone(),
        }
    }
}

impl<T> WriteQueue<T> {
    // Return if the item is put into queue or pending tasks.
    // Note if the task is put into pending tasks, it will be sent to queue when the queue is not full.
    pub fn push(&self, item: T) -> bool {
        let mut inner = self.inner.lock();
        let item = match inner.queue.push(item) {
            Ok(_) => {
                if !inner.queue.working() {
                    inner.queue.mark_working();
                    drop(inner);
                    let _ = self.working_notifier.notify();
                }
                return true;
            }
            Err(item) => item,
        };

        // The queue is full now
        inner.queue.mark_stuck();
        let pending = PendingTask {
            data: Some(item),
            waiter: None,
        };
        inner.pending_tasks.push_back(pending);
        Self::log_write_state("push-to-pending", inner.debug_state());
        false
    }

    // Return if the item is put into queue or pending tasks.
    // Note if the task is put into pending tasks, it will be sent to queue when the queue is not full.
    pub fn push_without_notify(&self, item: T) -> bool {
        let mut inner = self.inner.lock();
        let item = match inner.queue.push(item) {
            Ok(_) => return true,
            Err(item) => item,
        };

        // The queue is full now
        inner.queue.mark_stuck();
        let pending = PendingTask {
            data: Some(item),
            waiter: None,
        };
        inner.pending_tasks.push_back(pending);
        Self::log_write_state("push-to-pending", inner.debug_state());
        false
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock();
        inner.queue.is_empty()
    }

    // If peer is not working, notify it and mark it working.
    // Return notified
    pub fn notify_manually(&self) -> bool {
        let inner = self.inner.lock();

        if inner.queue.working() {
            return false;
        }

        inner.queue.mark_working();
        drop(inner);
        let _ = self.working_notifier.notify();
        true
    }

    pub fn push_with_awaiter(&self, item: T) -> PushResult {
        let mut inner = self.inner.lock();

        let item = match inner.queue.push(item) {
            Ok(_) => {
                if !inner.queue.working() {
                    inner.queue.mark_working();
                    drop(inner);
                    let _ = self.working_notifier.notify();
                }
                return PushResult::Ok;
            }
            Err(item) => item,
        };

        // The queue is full now
        inner.queue.mark_stuck();
        let waker_slot = Arc::new(Mutex::new(WakerSlot::None));
        let pending = PendingTask {
            data: Some(item),
            waiter: Some(waker_slot.clone()),
        };

        inner.pending_tasks.push_back(pending);
        Self::log_write_state("push-to-pending", inner.debug_state());
        PushResult::Pending(PushJoinHandle { waker_slot })
    }

    async fn unstuck_handler(self, mut unstuck_awaiter: Awaiter, mut tx: Sender<()>) {
        let mut exit = std::pin::pin!(tx.closed());
        loop {
            let mut debug_state = None;
            let mut debug_reason = "before-wait";
            let mut skip_wait = false;
            {
                let mut inner = self.inner.lock();

                while let Some(mut pending_task) = inner.pending_tasks.pop_front() {
                    let data = pending_task.data.take().unwrap();
                    match inner.queue.push(data) {
                        Ok(_) => {
                            if let Some(waiter) = pending_task.waiter {
                                waiter.lock().wake();
                            }
                        }
                        Err(data) => {
                            pending_task.data = Some(data);
                            inner.pending_tasks.push_front(pending_task);
                            break;
                        }
                    }
                }
                if !inner.queue.working() {
                    inner.queue.mark_working();
                    debug_state = Some(inner.debug_state());
                    debug_reason = "mark-working-and-notify";
                    let _ = self.working_notifier.notify();
                }
                if !inner.pending_tasks.is_empty() {
                    inner.queue.mark_stuck();
                    if !inner.queue.is_full() {
                        debug_state = Some(inner.debug_state());
                        debug_reason = "pending-continue";
                        skip_wait = true;
                    }
                }
                if debug_state.is_none() {
                    debug_state = Some(inner.debug_state());
                }
            }

            if let Some(state) = debug_state {
                Self::log_write_state(debug_reason, state);
            }

            if skip_wait {
                continue;
            }

            select! {
                _ = unstuck_awaiter.wait() => (),
                _ = &mut exit => {
                    let state = self.inner.lock().debug_state();
                    Self::log_write_state("exit", state);
                    return;
                }
            }
        }
    }
}

pub struct WriteQueueInner<T> {
    queue: Queue<T>,
    pending_tasks: VecDeque<PendingTask<T>>,
    _guard: Receiver<()>,
}

#[derive(Clone, Copy)]
struct DebugState {
    queue_len: usize,
    pending_len: usize,
    working: bool,
    stuck: bool,
    full: bool,
}

impl<T> WriteQueueInner<T> {
    fn debug_state(&self) -> DebugState {
        DebugState {
            queue_len: self.queue.len(),
            pending_len: self.pending_tasks.len(),
            working: self.queue.working(),
            stuck: self.queue.stuck(),
            full: self.queue.is_full(),
        }
    }
}

impl<T> WriteQueue<T> {
    #[inline]
    pub fn meta(&self) -> QueueMeta {
        self.inner.lock().queue.meta()
    }

    fn log_write_state(reason: &str, state: DebugState) {
        println!(
            "[rust][write][{reason}] queue_len={} pending={} working={} stuck={} full={}",
            state.queue_len, state.pending_len, state.working, state.stuck, state.full
        );
    }
}

struct PendingTask<T> {
    // always Some, Option is for taking temporary
    data: Option<T>,
    waiter: Option<Arc<Mutex<WakerSlot>>>,
}

enum WakerSlot {
    None,
    Some(Waker),
    Finished,
}

impl WakerSlot {
    fn wake(&mut self) {
        if let WakerSlot::Some(w) = mem::replace(self, Self::Finished) {
            w.wake();
        }
    }

    fn set_waker(&mut self, w: &Waker) -> bool {
        match self {
            WakerSlot::None => *self = WakerSlot::Some(w.to_owned()),
            WakerSlot::Some(old_waker) => old_waker.clone_from(w),
            WakerSlot::Finished => return true,
        }
        false
    }
}

pub struct Queue<T> {
    buffer_ptr: *mut MaybeUninit<T>,
    buffer_len: usize,

    head_ptr: *mut AtomicU64,
    tail_ptr: *mut AtomicU64,
    working_ptr: *mut AtomicU32,
    stuck_ptr: *mut AtomicU32,

    working_fd: RawFd,
    unstuck_fd: RawFd,

    do_drop: bool,
}

unsafe impl<T: Send> Send for Queue<T> {}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct QueueMeta {
    pub buffer_ptr: usize,
    pub buffer_len: usize,
    pub head_ptr: usize,
    pub tail_ptr: usize,
    pub working_ptr: usize,
    pub stuck_ptr: usize,
    pub working_fd: RawFd,
    pub unstuck_fd: RawFd,
}

unsafe impl<T: Sync> Sync for Queue<T> {}

impl<T> Queue<T> {
    pub fn new(size: usize) -> Result<(Self, QueueMeta), io::Error> {
        let buffer = unsafe {
            let mut v = Vec::<MaybeUninit<T>>::with_capacity(size);
            v.set_len(size);
            v.into_boxed_slice()
        };
        let buffer_slice = Box::leak(buffer);

        let head_ptr = Box::leak(Box::new(AtomicU64::new(0)));
        let tail_ptr = Box::leak(Box::new(AtomicU64::new(0)));
        let working_ptr = Box::leak(Box::new(AtomicU32::new(0)));
        let stuck_ptr = Box::leak(Box::new(AtomicU32::new(0)));

        let (working_fd, working_fd_peer) = new_pair()?;
        let (unstuck_fd, unstuck_fd_peer) = new_pair()?;

        let queue = Self {
            buffer_ptr: buffer_slice.as_mut_ptr(),
            buffer_len: size,
            head_ptr,
            tail_ptr,
            working_ptr,
            stuck_ptr,
            working_fd,
            unstuck_fd,
            do_drop: true,
        };
        let meta = QueueMeta {
            buffer_ptr: queue.buffer_ptr as _,
            buffer_len: queue.buffer_len,
            head_ptr: queue.head_ptr as _,
            tail_ptr: queue.tail_ptr as _,
            working_ptr: queue.working_ptr as _,
            stuck_ptr: queue.stuck_ptr as _,
            working_fd: working_fd_peer,
            unstuck_fd: unstuck_fd_peer,
        };

        Ok((queue, meta))
    }

    /// # Safety
    /// Must make sure the meta is valid until the Queue is dropped
    pub unsafe fn new_from_meta(meta: &QueueMeta) -> Result<Self, io::Error> {
        let buffer_slice =
            std::slice::from_raw_parts_mut(meta.buffer_ptr as *mut MaybeUninit<T>, meta.buffer_len);
        let size = buffer_slice.len();
        let head_ptr = meta.head_ptr as *mut AtomicU64;
        let tail_ptr = meta.tail_ptr as *mut AtomicU64;
        let working_ptr = meta.working_ptr as *mut AtomicU32;
        let stuck_ptr = meta.stuck_ptr as *mut AtomicU32;
        let working_fd = meta.working_fd;
        let unstuck_fd = meta.unstuck_fd;
        Ok(Self {
            buffer_ptr: buffer_slice.as_mut_ptr(),
            buffer_len: size,
            head_ptr,
            tail_ptr,
            working_ptr,
            stuck_ptr,
            working_fd,
            unstuck_fd,
            do_drop: false,
        })
    }

    #[inline]
    pub fn is_memory_owner(&self) -> bool {
        self.do_drop
    }

    #[inline]
    pub fn meta(&self) -> QueueMeta {
        QueueMeta {
            buffer_ptr: self.buffer_ptr as _,
            buffer_len: self.buffer_len,
            head_ptr: self.head_ptr as _,
            tail_ptr: self.tail_ptr as _,
            working_ptr: self.working_ptr as _,
            stuck_ptr: self.stuck_ptr as _,
            working_fd: self.working_fd,
            unstuck_fd: self.unstuck_fd,
        }
    }

    pub fn read(self) -> ReadQueue<T> {
        let unstuck_notifier = unsafe { Notifier::from_raw_fd(self.unstuck_fd) };
        ReadQueue {
            queue: self,
            unstuck_notifier,
            tokio_handle: None,
        }
    }

    pub fn read_with_tokio_handle(self, tokio_handle: tokio::runtime::Handle) -> ReadQueue<T> {
        let unstuck_notifier = unsafe { Notifier::from_raw_fd(self.unstuck_fd) };
        ReadQueue {
            queue: self,
            unstuck_notifier,
            tokio_handle: Some(tokio_handle),
        }
    }

    pub fn write(self) -> Result<WriteQueue<T>, io::Error>
    where
        T: Send + 'static,
    {
        let working_notifier = unsafe { Notifier::from_raw_fd(self.working_fd) };
        let unstuck_awaiter = unsafe { Awaiter::from_raw_fd(self.unstuck_fd) }?;

        let (tx, rx) = channel();
        let wq = WriteQueue {
            inner: Arc::new(Mutex::new(WriteQueueInner {
                queue: self,
                pending_tasks: VecDeque::new(),
                _guard: rx,
            })),
            working_notifier: Arc::new(working_notifier),
        };

        spawn(wq.clone().unstuck_handler(unstuck_awaiter, tx));

        Ok(wq)
    }

    pub fn write_with_tokio_handle(
        self,
        tokio_handle: &tokio::runtime::Handle,
    ) -> Result<WriteQueue<T>, io::Error>
    where
        T: Send + 'static,
    {
        let working_notifier = unsafe { Notifier::from_raw_fd(self.working_fd) };
        let unstuck_awaiter = unsafe { Awaiter::from_raw_fd(self.unstuck_fd) }?;

        let (tx, rx) = channel();
        let wq = WriteQueue {
            inner: Arc::new(Mutex::new(WriteQueueInner {
                queue: self,
                pending_tasks: VecDeque::new(),
                _guard: rx,
            })),
            working_notifier: Arc::new(working_notifier),
        };

        tokio_handle.spawn(wq.clone().unstuck_handler(unstuck_awaiter, tx));

        Ok(wq)
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        if self.do_drop {
            unsafe {
                let slice = std::slice::from_raw_parts_mut(self.buffer_ptr, self.buffer_len);
                let _ = Box::from_raw(slice as *mut [MaybeUninit<T>]);
                let _ = Box::from_raw(self.head_ptr);
                let _ = Box::from_raw(self.tail_ptr);
                let _ = Box::from_raw(self.working_ptr);
                let _ = Box::from_raw(self.stuck_ptr);
                let _ = Notifier::from_raw_fd(self.unstuck_fd);
                let _ = Notifier::from_raw_fd(self.working_fd);
            }
        }
    }
}

pub enum PushResult {
    Ok,
    Pending(PushJoinHandle),
}

pub struct PushJoinHandle {
    waker_slot: Arc<Mutex<WakerSlot>>,
}

impl Future for PushJoinHandle {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut slot = self.waker_slot.lock();
        if slot.set_waker(cx.waker()) {
            return std::task::Poll::Ready(());
        }
        std::task::Poll::Pending
    }
}

impl<T> Queue<T> {
    pub fn len(&self) -> usize {
        let shead = unsafe { &*self.head_ptr };
        let stail = unsafe { &*self.tail_ptr };
        (stail.load(Ordering::Acquire) - shead.load(Ordering::Acquire)) as usize
    }

    pub fn is_empty(&self) -> bool {
        let shead = unsafe { &*self.head_ptr };
        let stail = unsafe { &*self.tail_ptr };
        stail.load(Ordering::Acquire) == shead.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        let shead = unsafe { &*self.head_ptr };
        let stail = unsafe { &*self.tail_ptr };
        stail.load(Ordering::Acquire) - shead.load(Ordering::Acquire) == self.buffer_len as u64
    }

    fn push(&mut self, item: T) -> Result<(), T> {
        let shead = unsafe { &*self.head_ptr };
        let stail = unsafe { &*self.tail_ptr };

        let tail = stail.load(Ordering::Relaxed);
        if tail - shead.load(Ordering::Acquire) == self.buffer_len as u64 {
            return Err(item);
        }

        unsafe {
            (*self
                .buffer_ptr
                .add((tail % (self.buffer_len as u64)) as usize))
            .write(item);
        }
        stail.store(tail + 1, Ordering::Release);
        Ok(())
    }

    fn pop(&mut self) -> Option<T> {
        let shead = unsafe { &*self.head_ptr };
        let stail = unsafe { &*self.tail_ptr };

        let head = shead.load(Ordering::Relaxed);
        if head == stail.load(Ordering::Acquire) {
            return None;
        }

        let item = unsafe {
            (*self
                .buffer_ptr
                .add((head % (self.buffer_len as u64)) as usize))
            .assume_init_read()
        };
        shead.store(head + 1, Ordering::Release);
        Some(item)
    }

    #[inline]
    fn mark_unworking(&self) -> bool {
        unsafe { &*self.working_ptr }.store(0, Ordering::Release);
        if self.is_empty() {
            return true;
        }
        self.mark_working();
        false
    }

    #[inline]
    fn mark_working(&self) {
        unsafe { &*self.working_ptr }.store(1, Ordering::Release);
    }

    #[inline]
    fn working(&self) -> bool {
        unsafe { &*self.working_ptr }.load(Ordering::Acquire) == 1
    }

    #[inline]
    fn mark_unstuck(&self) {
        unsafe { &*self.stuck_ptr }.store(0, Ordering::Release);
    }

    #[inline]
    fn mark_stuck(&self) {
        unsafe { &*self.stuck_ptr }.store(1, Ordering::Release);
    }

    #[inline]
    fn stuck(&self) -> bool {
        unsafe { &*self.stuck_ptr }.load(Ordering::Acquire) == 1
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn demo_wake() {
        let (mut tx, mut rx) = channel::<()>();

        let (q_read, meta) = Queue::<u8>::new(1024).unwrap();
        let q_write = unsafe { Queue::<u8>::new_from_meta(&meta) }.unwrap();
        let q_read = q_read.read();
        let q_write = q_write.write().unwrap();

        let _guard = q_read
            .run_handler(move |item| {
                if item == 2 {
                    rx.close();
                }
            })
            .unwrap();

        q_write.push(1);
        sleep(Duration::from_secs(1)).await;
        q_write.push(2);
        tx.closed().await;
    }

    #[tokio::test]
    async fn demo_stuck() {
        let (mut tx, mut rx) = channel::<()>();

        let (q_read, meta) = Queue::<u8>::new(1).unwrap();
        let q_write = unsafe { Queue::<u8>::new_from_meta(&meta) }.unwrap();
        let q_read = q_read.read();
        let q_write = q_write.write().unwrap();

        let _guard = q_read
            .run_handler(move |item| {
                if item == 4 {
                    rx.close();
                }
            })
            .unwrap();
        println!("pushed {}", q_write.push(1));
        println!("pushed {}", q_write.push(2));
        println!("pushed {}", q_write.push(3));
        println!("pushed {}", q_write.push(4));
        sleep(Duration::from_secs(1)).await;

        tx.closed().await;
    }
}

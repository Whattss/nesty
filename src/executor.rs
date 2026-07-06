use std::{pin::Pin, sync::{Arc, Mutex, OnceLock, atomic::{AtomicUsize, Ordering}}, task::{Context, Poll, RawWaker, RawWakerVTable, Waker}};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::sync::{Parker, Unparker};

// Round-robin index for picking which worker thread to unpark.
pub static NEXT_UNPARKER: AtomicUsize = AtomicUsize::new(0);
pub static UNPARKERS: OnceLock<Arc<Vec<Unparker>>> = OnceLock::new();
pub static SPAWNER: OnceLock<Spawner> = OnceLock::new();

/// A heap-allocated future plus a reference back to the injector so it can
/// re-enqueue itself when woken.
pub struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
    injector: Arc<Injector<Arc<Task>>>,
}

/// Work-stealing thread pool. One OS thread per logical CPU core.
pub struct Executor {
    injector: Arc<Injector<Arc<Task>>>,
    stealers: Arc<Vec<Stealer<Arc<Task>>>>,
    pub unparkers: Arc<Vec<Unparker>>,
    num_threads: usize,
}

/// Cheap handle for spawning tasks from anywhere, including async contexts.
pub struct Spawner {
    injector: Arc<Injector<Arc<Task>>>,
    unparkers: Arc<Vec<Unparker>>,
}

impl Spawner {
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            injector: self.injector.clone(),
        });
        self.injector.push(task);
        if let Some(unparkers) = UNPARKERS.get() {
            let i = NEXT_UNPARKER.fetch_add(1, Ordering::Relaxed) % unparkers.len();
            self.unparkers[i].unpark();
        }
    }
}

impl Executor {
    pub fn new() -> (Executor, Vec<Worker<Arc<Task>>>, Vec<Parker>, Spawner) {
        let num_threads = std::thread::available_parallelism().unwrap().get();

        let workers: Vec<Worker<Arc<Task>>> =
            (0..num_threads).map(|_| Worker::new_fifo()).collect();

        let stealers: Arc<Vec<Stealer<Arc<Task>>>> =
            Arc::new(workers.iter().map(|w| w.stealer()).collect());

        let parkers: Vec<Parker> = (0..num_threads).map(|_| Parker::new()).collect();
        let unparkers: Vec<Unparker> = parkers.iter().map(|p| p.unparker().clone()).collect();

        let injector: Arc<Injector<Arc<Task>>> = Arc::new(Injector::new());

        (
            Executor {
                injector: injector.clone(),
                stealers,
                unparkers: Arc::new(unparkers.clone()),
                num_threads,
            },
            workers,
            parkers,
            Spawner {
                injector,
                unparkers: Arc::new(unparkers),
            },
        )
    }

    /// Spawns one OS thread per worker and blocks until they all exit (never,
    /// in practice — the threads loop forever).
    pub fn run(self, workers: Vec<Worker<Arc<Task>>>, parkers: Vec<Parker>) {
        let handles: Vec<_> = workers
            .into_iter()
            .zip(parkers)
            .map(|(worker, parker)| {
                let injector = self.injector.clone();
                let stealers = self.stealers.clone();

                std::thread::spawn(move || loop {
                    // 1. local queue  2. global injector  3. steal from siblings
                    let task = worker
                        .pop()
                        .or_else(|| loop {
                            match injector.steal_batch_and_pop(&worker) {
                                Steal::Success(t) => break Some(t),
                                Steal::Retry => continue,
                                Steal::Empty => break None,
                            }
                        })
                        .or_else(|| {
                            stealers
                                .iter()
                                .filter_map(|s| match s.steal() {
                                    Steal::Success(t) => Some(t),
                                    _ => None,
                                })
                                .next()
                        });

                    match task {
                        Some(task) => process(task),
                        None => parker.park(),
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}

/// Polls a task once. If it returns Pending the waker will re-enqueue it
/// when the reactor fires.
pub fn process(task: Arc<Task>) {
    let ptr = Arc::into_raw(task.clone()) as *const ();
    let raw = RawWaker::new(ptr, &TASK_VTABLE);
    let waker = unsafe { Waker::from_raw(raw) };
    let mut cx = Context::from_waker(&waker);
    let mut future = task.future.lock().unwrap();
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(()) => {}
        Poll::Pending => {}
    }
}

// Manual RawWakerVTable — needed because Task isn't Unpin and we manage the
// Arc refcount by hand to avoid an extra allocation.
pub const TASK_VTABLE: RawWakerVTable =
    RawWakerVTable::new(task_clone, task_wake, task_wake_by_ref, task_drop);

pub fn task_clone(ptr: *const ()) -> RawWaker {
    unsafe {
        let arc = Arc::from_raw(ptr as *const Task);
        let cloned = arc.clone();
        std::mem::forget(arc); // don't drop the original
        RawWaker::new(Arc::into_raw(cloned) as *const (), &TASK_VTABLE)
    }
}

pub fn task_wake(ptr: *const ()) {
    unsafe {
        let arc = Arc::from_raw(ptr as *const Task);
        arc.injector.push(arc.clone());
        if let Some(unparkers) = UNPARKERS.get() {
            let i = NEXT_UNPARKER.fetch_add(1, Ordering::Relaxed) % unparkers.len();
            unparkers[i].unpark();
        }
    } // arc is dropped here, balancing the from_raw above
}

pub fn task_wake_by_ref(ptr: *const ()) {
    unsafe {
        let arc = Arc::from_raw(ptr as *const Task);
        arc.injector.push(arc.clone());
        std::mem::forget(arc); // caller still owns the original pointer
        if let Some(unparkers) = UNPARKERS.get() {
            let i = NEXT_UNPARKER.fetch_add(1, Ordering::Relaxed) % unparkers.len();
            unparkers[i].unpark();
        }
    }
}

pub fn task_drop(ptr: *const ()) {
    unsafe {
        drop(Arc::from_raw(ptr as *const Task));
    }
}

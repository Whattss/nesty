use dashmap::DashMap;
use mio::{Events, Interest, Poll as MioPoll, Token};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::Waker;
use std::time::Instant;

use crate::executor::{NEXT_UNPARKER, UNPARKERS};

static NEXT_TOKEN: AtomicUsize = AtomicUsize::new(0);
static REACTOR: OnceLock<Reactor> = OnceLock::new();

/// Allocates a unique mio Token for each registered I/O source.
pub fn next_token() -> Token {
    Token(NEXT_TOKEN.fetch_add(1, Ordering::Relaxed))
}

pub fn reactor() -> &'static Reactor {
    REACTOR.get_or_init(Reactor::new)
}

// Stored in a min-heap (reversed Ord so BinaryHeap becomes a min-heap by deadline).
pub struct TimerEntry {
    pub deadline: Instant,
    pub waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reversed so the earliest deadline sits at the top of the heap.
        other.deadline.cmp(&self.deadline)
    }
}

/// Drives I/O readiness (via mio/epoll) and timer expiry on a dedicated thread.
/// Wakers stored here are fired once the event arrives, which causes the executor
/// to re-poll the corresponding future.
pub struct Reactor {
    poll: Mutex<MioPoll>,
    registry: mio::Registry,
    wakers: DashMap<Token, Waker>,   // lock-free; concurrent inserts from many tasks
    timers: Mutex<BinaryHeap<TimerEntry>>,
    wakeup: mio::Waker,              // interrupts poll() when a timer is registered
}

impl Reactor {
    pub fn new() -> Self {
        let poll = MioPoll::new().unwrap();
        let registry = poll.registry().try_clone().unwrap();
        let wakeup = mio::Waker::new(poll.registry(), Token(usize::MAX)).unwrap();
        Reactor {
            poll: Mutex::new(poll),
            registry,
            wakers: DashMap::new(),
            timers: Mutex::new(BinaryHeap::new()),
            wakeup,
        }
    }

    /// First registration of an I/O source. Subsequent polls should use `reregister`.
    pub fn register(&self, source: &mut impl mio::event::Source, token: Token, waker: Waker) {
        self.registry
            .register(source, token, Interest::READABLE | Interest::WRITABLE)
            .unwrap();
        self.wakers.insert(token, waker);
    }

    /// Updates the waker for an already-registered source.
    pub fn reregister(&self, source: &mut impl mio::event::Source, token: Token, waker: Waker) {
        self.registry
            .reregister(source, token, Interest::READABLE | Interest::WRITABLE)
            .unwrap();
        self.wakers.insert(token, waker);
    }

    pub fn register_timer(&self, deadline: Instant, waker: Waker) {
        self.timers.lock().unwrap().push(TimerEntry { deadline, waker });
        // Interrupt the current poll() so the new timeout is respected immediately.
        self.wakeup.wake().unwrap();
    }

    /// Blocking event loop — meant to run on its own thread.
    pub fn run(&self) {
        let mut events = Events::with_capacity(64);
        loop {
            // Block until the next timer deadline or until an I/O event arrives.
            let timeout = {
                let timers = self.timers.lock().unwrap();
                timers
                    .peek()
                    .map(|t| t.deadline.saturating_duration_since(Instant::now()))
            };

            self.poll.lock().unwrap().poll(&mut events, timeout).unwrap();

            let now = Instant::now();
            let mut timers = self.timers.lock().unwrap();
            while let Some(entry) = timers.peek() {
                if entry.deadline <= now {
                    let entry = timers.pop().unwrap();
                    entry.waker.wake();
                    if let Some(unparkers) = UNPARKERS.get() {
                        let i = NEXT_UNPARKER.fetch_add(1, Ordering::Relaxed) % unparkers.len();
                        unparkers[i].unpark();
                    }
                } else {
                    break;
                }
            }

            for event in &events {
                if let Some((_, waker)) = self.wakers.remove(&event.token()) {
                    waker.wake()
                }
            }
        }
    }
}

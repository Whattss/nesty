use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::reactor::reactor;

/// Future that completes after `duration` has elapsed.
/// On the first poll where the deadline hasn't passed yet, it registers itself
/// with the reactor so it gets woken at the right time.
pub struct Sleep {
    deadline: Instant,
    registered: bool,
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Sleep {
            deadline: Instant::now() + duration,
            registered: false,
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if Instant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            if !self.registered {
                reactor().register_timer(self.deadline, cx.waker().clone());
                self.registered = true;
            }
            Poll::Pending
        }
    }
}

pub fn sleep(duration: Duration) -> Sleep {
    Sleep::new(duration)
}

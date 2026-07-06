use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};

use nesty::executor::{Executor, SPAWNER, UNPARKERS};
use nesty::net::{AsyncListener, AsyncStream};
use nesty::pool::BufferPool;
use nesty::reactor::reactor;

static BUFFER_POOL: OnceLock<BufferPool> = OnceLock::new();

fn main() {
    let (executor, workers, parkers, spawner) = Executor::new();
    UNPARKERS.get_or_init(|| executor.unparkers.clone());
    SPAWNER.get_or_init(|| spawner);
    BUFFER_POOL.get_or_init(|| BufferPool::new(1024, 1000));

    SPAWNER.get().unwrap().spawn(async move {
        let mut listener = AsyncListener::new("127.0.0.1:8082".parse().unwrap());
        loop {
            let (stream, _addr) = listener.accept().await;
            SPAWNER.get().unwrap().spawn(async move {
                let mut stream = AsyncStream::new(stream);
                let mut buf = BUFFER_POOL.get().unwrap().get();
                let n = stream.read(&mut buf).await.unwrap();
                let _ = n;
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello, World!";
                let mut response_bytes = response.as_bytes().to_vec();
                stream.write(&mut response_bytes).await.unwrap();
                BUFFER_POOL.get().unwrap().return_buf(buf);
            });
        }
    });

    std::thread::spawn(|| reactor().run());

    executor.run(workers, parkers);
}

/// Yields control back to the executor once, then completes on the next poll.
/// Useful for cooperatively giving other tasks a chance to run.
struct YieldOnce {
    pending: bool,
}

impl YieldOnce {
    fn new() -> Self {
        YieldOnce { pending: true }
    }
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if !self.pending {
            Poll::Ready(())
        } else {
            self.pending = false;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

use mio::Token;
use mio::net::{TcpListener, TcpStream};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::reactor::{next_token, reactor};

/// Async wrapper around a non-blocking `TcpListener`.
pub struct AsyncListener {
    listener: TcpListener,
    token: Token,
    registered: bool,
}

impl AsyncListener {
    pub fn new(addr: SocketAddr) -> Self {
        AsyncListener {
            listener: TcpListener::bind(addr).unwrap(),
            token: next_token(),
            registered: false,
        }
    }

    pub fn accept(&mut self) -> Accept<'_> {
        Accept { listener: self }
    }
}

pub struct Accept<'a> {
    listener: &'a mut AsyncListener,
}

impl<'a> Future for Accept<'a> {
    type Output = (TcpStream, SocketAddr);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.listener.listener.accept() {
            Ok((stream, addr)) => Poll::Ready((stream, addr)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                let token = self.listener.token;
                // re-register on subsequent polls to update the stored waker
                if self.listener.registered {
                    reactor().reregister(&mut self.listener.listener, token, cx.waker().clone());
                } else {
                    reactor().register(&mut self.listener.listener, token, cx.waker().clone());
                    self.listener.registered = true;
                }
                Poll::Pending
            }
            Err(e) => panic!("{}", e),
        }
    }
}

/// Async wrapper around a non-blocking `TcpStream`.
pub struct AsyncStream {
    stream: TcpStream,
    token: Token,
    registered: bool,
}

impl AsyncStream {
    pub fn new(stream: TcpStream) -> Self {
        AsyncStream {
            stream,
            token: next_token(),
            registered: false,
        }
    }

    pub fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Read<'a> {
        Read { stream: self, buf }
    }

    pub fn write<'a>(&'a mut self, buf: &'a mut [u8]) -> Write<'a> {
        Write { stream: self, buf }
    }
}

pub struct Read<'a> {
    stream: &'a mut AsyncStream,
    buf: &'a mut [u8],
}

impl<'a> Future for Read<'a> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use std::io::Read;
        let this = self.get_mut();
        match this.stream.stream.read(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                let token = this.stream.token;
                if !this.stream.registered {
                    reactor().register(&mut this.stream.stream, token, cx.waker().clone());
                    this.stream.registered = true;
                } else {
                    reactor().reregister(&mut this.stream.stream, token, cx.waker().clone());
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct Write<'a> {
    stream: &'a mut AsyncStream,
    buf: &'a mut [u8],
}

impl<'a> Future for Write<'a> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use std::io::Write;
        let this = self.get_mut();
        match this.stream.stream.write(this.buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                let token = this.stream.token;
                if !this.stream.registered {
                    reactor().register(&mut this.stream.stream, token, cx.waker().clone());
                    this.stream.registered = true;
                } else {
                    reactor().reregister(&mut this.stream.stream, token, cx.waker().clone());
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

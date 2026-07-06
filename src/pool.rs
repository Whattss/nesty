use std::sync::Mutex;

/// Pre-allocated buffer pool to avoid a heap allocation on every request.
/// Falls back to allocating a fresh buffer if the pool is empty.
pub struct BufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
    buf_size: usize,
}

impl BufferPool {
    pub fn new(size: usize, count: usize) -> Self {
        BufferPool {
            buffers: Mutex::new((0..count).map(|_| vec![0u8; size]).collect()),
            buf_size: size,
        }
    }

    pub fn get(&self) -> Vec<u8> {
        self.buffers
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| vec![0u8; self.buf_size])
    }

    pub fn return_buf(&self, buf: Vec<u8>) {
        self.buffers.lock().unwrap().push(buf);
    }
}

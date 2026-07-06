# nesty

A minimal async runtime built from scratch in Rust. No tokio, no async-std — just epoll, a work-stealing executor, and enough plumbing to run a concurrent TCP server.

## What's inside

| Module | Responsibility |
|---|---|
| `executor` | Work-stealing thread pool (one thread per core) with a manual waker vtable |
| `reactor` | mio-backed I/O event loop + timer heap, runs on a dedicated thread |
| `net` | `AsyncListener` and `AsyncStream` — non-blocking TCP with futures |
| `timer` | `Sleep` future that integrates with the reactor |
| `pool` | Pre-allocated buffer pool to avoid per-request heap allocations |

## How it works

```
main()
 ├── initializes executor, spawner, buffer pool
 ├── spawns reactor thread  ← drives epoll + timers
 └── runs executor          ← work-stealing loop

task is spawned
 └── executor polls it
      └── future returns Pending
           └── registers waker with reactor
                └── reactor fires event
                     └── waker re-enqueues task
                          └── executor polls again → Ready
```

Tasks are heap-allocated futures wrapped in `Arc<Task>`. The waker vtable manages the refcount manually so re-enqueueing a task is just an `Arc::clone` + a push into the injector queue.

## Running

```bash
cargo run --release
```

Starts an HTTP server on `127.0.0.1:8082` that responds with `Hello, World!` to any request.

```bash
curl http://127.0.0.1:8082/
# Hello, World!
```

## Dependencies

- [`mio`](https://github.com/tokio-rs/mio) — cross-platform epoll/kqueue abstraction
- [`crossbeam-deque`](https://github.com/crossbeam-rs/crossbeam) — lock-free work-stealing queues
- [`crossbeam-utils`](https://github.com/crossbeam-rs/crossbeam) — `Parker`/`Unparker` for efficient thread sleeping
- [`dashmap`](https://github.com/xacrimon/dashmap) — concurrent hashmap for waker storage

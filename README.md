[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-brightgreen.svg)](https://github.com/ArunaStorage/synevi/blob/main/LICENSE-APACHE)
[![License](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/ArunaStorage/synevi/blob/main/LICENSE-MIT)
![CI](https://github.com/ArunaStorage/synevi/actions/workflows/test.yml/badge.svg)
[![Codecov](https://codecov.io/github/ArunaStorage/synevi/coverage.svg?branch=main)](https://codecov.io/gh/ArunaStorage/synevi)

# Synevi
Synevi (greek: συνέβει - "it happened") is a leaderless, strict serializable, embeddable event store for event sourcing. 

It is based on Apache Cassandras [Accord](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=1&modificationDate=1630847737000&api=v2) consensus algorithm which is a leaderless concurrency optimized variant of EPaxos.

For a more detailed explanation of the underlying architecture please visit our [architecture documentation](./docs/architecture.md).

## Features

- **Leaderless**: In contrast to many other eventstores *synevi* does not rely on a single elected leader, every node can act as a coordinator in parallel
- **Embedabble**: Synevi is designed to be directly embedded into your server application, transforming it into a fast and secure distributed system without additional network and maintenance overhead
- **Event Sourcing**: Built with event sourcing in mind to distribute your application state as a chain of events enabling advanced auditing, vetting and replay functionalities
- ...

## Usage

### Embedded

## Getting Started with Synevi

Synevi provides a distributed event storage system that ensures consistent event ordering across your network. You can get started with either gRPC networking (the default) and your choice of persistent LMDB storage or in-memory storage to match your needs.

At its core, Synevi works by distributing events through coordinator nodes. When an event is sent to any coordinator, Synevi ensures all nodes execute it in the same order, maintaining consistency across your system. To work with Synevi, you'll need to implement two key traits for your events.

First, define your events by implementing the `Transaction` trait:

```rust
pub trait Transaction: Debug + Clone + Send {
    type TxErr: Send + Serialize;
    type TxOk: Send + Serialize;
    
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized;
}
```

Then, implement the `Executor` trait to handle how your store processes these events:

```rust
#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction + Serialize;
    
    async fn execute(&self, transaction: Self::Tx) -> SyneviResult<Self>;
}
```

Here's a complete example that shows how to set up a Synevi node:

```rust
// Set up the network address
let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13000 + i)).unwrap();
let network = synevi::network::GrpcNetwork::new(
    socket_addr,
    format!("http://0.0.0.0:{}", 13000 + i),
    Ulid::new(),
    0u16,
);

// Choose your storage implementation
// For persistent storage using LMDB:
let test_path = format!("/dev/shm/test/");
fs::create_dir(&test_path).await.unwrap();
let store = LmdbStore::new(test_path, 0u16).unwrap();

// Or for in-memory storage:
// let store = MemStore::new(0u16).unwrap();

// Create your node using your executor (or the DummyExecutor that does nothing)
let node = Node::new(m, i as u16, network, DummyExecutor, store)
    .await
    .unwrap();
```

Synevi will automatically handle event distribution and ensure proper execution order across all functioning nodes in your network. The system is designed to be resilient, continuing to operate even if some nodes become unavailable. Whether you choose LMDB for persistence or in-memory storage for speed, the same consistency guarantees apply - all events are executed in the same order across every node in your network.

For production deployments, make sure your `Transaction` implementations include proper error handling and that your `Executor` implementation is thread-safe. Consider your network topology when setting up nodes, as it can impact system performance and reliability.

Note: Currently each node can handle only one type of executor, this can make it complicated when having many different types of events, for this you can use the excellent [typetag](https://github.com/dtolnay/typetag) crate that enables serialization of many types that implement shared behavior.

### As standalone Application (Coming soon)

## Feedback & Contributions

If you have any ideas, suggestions, or issues, please don't hesitate to open an issue and/or PR. Contributions to this project are always welcome ! We appreciate your help in making this project better. Please have a look at our [Contributor Guidelines](./CONTRIBUTING.md) for more information.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Synevi by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions. 


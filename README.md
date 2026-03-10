# Mmap-Fifo

A fast, persistent FIFO queue backed by memory-mapped files in Rust.

`mmap-fifo` provides a high-performance, disk-backed FIFO (First-In-First-Out) queue. It is designed for scenarios where you need to store more data than fits in RAM, or where data must persist across application restarts, while maintaining near-RAM speeds for operations.

## Features

- **Performance**: Leverages memory-mapped files for fast I/O.
- **Persistence**: Data is stored on disk and can be reloaded after a crash or restart.
- **Segmentation**: Uses multiple page files to manage disk space, automatically rotating and cleaning up old pages.
- **Type-safe**: Optional usage of `serde` and `postcard` for efficient serialization of any `Serialize` + `DeserializeOwned` types.
- **Iterators**: Supports both borrowed and consuming iterators.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
mmap-fifo = { version = "0.2.0", features = ["postcard"] }
serde = { version = "1.0", features = ["derive"] }
```

Using the `postcard` feature enables the `PostcardSerializer` which is the easiest way to use `MmapFifo` with any `serde`-compatible type.

## Usage

### Creating a new FIFO (with `serde` + `postcard`)

```rust
use mmap_fifo::{MmapFifo, PostcardSerializer};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MyData {
    id: u64,
    name: String,
}

fn main() -> anyhow::Result<()> {
    let base_path = "./my_fifo";
    let page_size = 1024 * 1024; // 1MB pages
    
    // Using PostcardSerializer (requires "postcard" feature)
    let mut fifo: MmapFifo<MyData, PostcardSerializer<MyData>> = 
        MmapFifo::new(base_path, page_size)?;
    
    let data = MyData { id: 42, name: "hello".to_string() };
    fifo.push(&data)?;
    
    assert_eq!(fifo.pop()?, Some(data));
    
    Ok(())
}
```

### Using a custom serializer

If you don't want to use `serde`, you can implement the `EntrySerializer` trait:

```rust
use mmap_fifo::{MmapFifo, EntrySerializer};

struct MySerializer;

impl EntrySerializer<u64> for MySerializer {
    type Error = std::io::Error;

    fn serialize(item: &u64) -> Result<Vec<u8>, Self::Error> {
        Ok(item.to_be_bytes().to_vec())
    }

    fn deserialize(bytes: &[u8]) -> Result<u64, Self::Error> {
        Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid u64")
        })?))
    }
}

fn main() -> anyhow::Result<()> {
    let mut fifo: MmapFifo<u64, MySerializer> = MmapFifo::new("./custom_fifo", 1024)?;
    fifo.push(&42)?;
    assert_eq!(fifo.pop()?, Some(42));
    Ok(())
}
```

### Loading an existing FIFO

```rust
use mmap_fifo::{MmapFifo, PostcardSerializer};

let base_path = "./my_fifo";
let page_size = 1024 * 1024;

// Load existing state from disk
let mut fifo: MmapFifo<u64, PostcardSerializer<u64>> = MmapFifo::load(base_path, page_size)?;
println!("FIFO length: {}", fifo.len());
```

### Iterating over elements

```rust
for item in fifo.iter() {
    match item {
        Ok(value) => println!("Got: {:?}", value),
        Err(e) => eprintln!("Error reading item: {}", e),
    }
}
```

### Draining the queue

The `drain` method returns an iterator that pops elements from the queue without consuming it.

```rust
for item in fifo.drain() {
    let value = item?;
    println!("Popped: {:?}", value);
}
assert_eq!(fifo.len(), 0);
```

### Visiting and Modifying elements in-place

The `visit` function allows you to iterate over all items in the queue and optionally modify them in-place, provided their serialized size remains the same.

```rust
// Replace all items with value 42 with 100
fifo.visit(|&item| {
    if item == 42 {
        Some(100)
    } else {
        None // Keep unchanged
    }
})?;
```

## Benchmarks

Comprehensive benchmarks are included using `criterion`. To run them:

```bash
cargo bench
```

Current benchmarks cover:
- `push`: Adding items to the queue.
- `pop`: Removing items from the queue.
- `iter`: Iterating over all items in a large queue.
- `load`: Opening and restoring the state of an existing queue from disk.

## License

GPL-3.0

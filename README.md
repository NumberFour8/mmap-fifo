# Mmap-Fifo

A fast, persistent FIFO queue backed by memory-mapped files in Rust.

`mmap-fifo` provides a high-performance, disk-backed FIFO (First-In-First-Out) queue. It is designed for scenarios where you need to store more data than fits in RAM, or where data must persist across application restarts, while maintaining near-RAM speeds for operations.

## Features

- **Performance**: Leverages memory-mapped files for fast I/O.
- **Persistence**: Data is stored on disk and can be reloaded after a crash or restart.
- **Segmentation**: Uses multiple page files to manage disk space, automatically rotating and cleaning up old pages.
- **Type-safe**: Uses `serde` and `postcard` for efficient serialization of any `Serialize` + `DeserializeOwned` types.
- **Iterators**: Supports both borrowed and consuming iterators.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
mmap-fifo = "0.1.0"
serde = { version = "1.0", features = ["derive"] }
```

## Usage

### Creating a new FIFO

```rust
use mmap_fifo::MmapFifo;

fn main() -> anyhow::Result<()> {
    let base_path = "./my_fifo";
    let page_size = 1024 * 1024; // 1MB pages
    
    let mut fifo: MmapFifo<u64> = MmapFifo::new(base_path, page_size)?;
    
    fifo.push(&42)?;
    fifo.push(&43)?;
    
    assert_eq!(fifo.pop()?, Some(42));
    assert_eq!(fifo.pop()?, Some(43));
    
    Ok(())
}
```

### Loading an existing FIFO

```rust
use mmap_fifo::MmapFifo;

let base_path = "./my_fifo";
let page_size = 1024 * 1024;

// Load existing state from disk
let mut fifo: MmapFifo<u64> = MmapFifo::load(base_path, page_size)?;
println!("FIFO length: {}", fifo.len());
```

### Iterating over elements

```rust
for item in fifo.iter() {
    match item {
        Ok(value) => println!("Got: {}", value),
        Err(e) => eprintln!("Error reading item: {}", e),
    }
}
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

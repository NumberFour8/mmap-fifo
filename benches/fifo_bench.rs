use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use mmap_fifo::MmapFifo;
use tempfile::TempDir;

struct Uint64BeSerializer;

impl mmap_fifo::EntrySerializer<u64> for Uint64BeSerializer {
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

struct U8ArraySerializer<const N: usize>(std::marker::PhantomData<[u8; N]>);

impl<const N: usize> mmap_fifo::EntrySerializer<[u8; N]> for U8ArraySerializer<N> {
    type Error = std::io::Error;

    fn serialize(item: &[u8; N]) -> Result<Vec<u8>, Self::Error> {
        Ok(item.to_vec())
    }

    fn deserialize(bytes: &[u8]) -> Result<[u8; N], Self::Error> {
        Ok(bytes
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid array"))?)
    }
}

fn bench_push(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut fifo: MmapFifo<u64, Uint64BeSerializer> = MmapFifo::new(temp_dir.path(), 64 * 1024).unwrap();

    c.bench_function("push_u64", |b| {
        b.iter(|| {
            fifo.push(black_box(&42u64)).unwrap();
        })
    });
}

fn bench_pop(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut fifo: MmapFifo<u64, Uint64BeSerializer> = MmapFifo::new(temp_dir.path(), 1024 * 1024).unwrap();

    // Fill the FIFO first
    for i in 0..10000 {
        fifo.push(&(i as u64)).unwrap();
    }

    c.bench_function("pop_u64", |b| {
        b.iter(|| {
            black_box(fifo.pop().unwrap());
        })
    });
}

fn bench_iter(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut fifo: MmapFifo<u64, Uint64BeSerializer> = MmapFifo::new(temp_dir.path(), 1024 * 1024).unwrap();

    // Fill the FIFO
    for i in 0..10000 {
        fifo.push(&(i as u64)).unwrap();
    }

    c.bench_function("iter_10000_u64", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for item in fifo.iter() {
                sum += item.unwrap();
            }
            black_box(sum);
        })
    });
}

fn bench_load(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    let page_size = 64 * 1024;

    {
        let mut fifo: MmapFifo<u64, Uint64BeSerializer> = MmapFifo::new(path, page_size).unwrap();
        for i in 0..1000 {
            fifo.push(&(i as u64)).unwrap();
        }
    }

    c.bench_function("load_1000_u64", |b| {
        b.iter(|| {
            let fifo: MmapFifo<u64, Uint64BeSerializer> =
                MmapFifo::load(black_box(path), black_box(page_size)).unwrap();
            black_box(fifo);
        })
    });
}

fn bench_visit(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut fifo: MmapFifo<[u8; 8], U8ArraySerializer<8>> = MmapFifo::new(temp_dir.path(), 1024 * 1024).unwrap();

    // Fill the FIFO
    for i in 0..10000 {
        fifo.push(&(i as u64).to_le_bytes()).unwrap();
    }

    let mut g = c.benchmark_group("visit_10000_u64_array");

    g.bench_function("read_only", |b| {
        b.iter(|| {
            fifo.visit(|_item| None).unwrap();
        })
    });

    g.bench_function("read_write", |b| {
        b.iter(|| {
            fifo.visit(|item| {
                let val = u64::from_le_bytes(*item);
                Some(val.wrapping_add(1).to_le_bytes())
            })
            .unwrap();
        })
    });

    g.finish();
}

fn bench_drain(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut fifo: MmapFifo<u64, Uint64BeSerializer> = MmapFifo::new(temp_dir.path(), 1024 * 1024).unwrap();

    c.bench_function("drain_10000_u64", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::ZERO;
            for _ in 0..iters {
                // Fill the FIFO
                for i in 0..10000 {
                    fifo.push(&(i as u64)).unwrap();
                }

                let start = std::time::Instant::now();
                for item in fifo.drain() {
                    black_box(item.unwrap());
                }
                total_duration += start.elapsed();
            }
            total_duration
        })
    });
}

criterion_group!(
    benches,
    bench_push,
    bench_pop,
    bench_iter,
    bench_load,
    bench_visit,
    bench_drain
);
criterion_main!(benches);

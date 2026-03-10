use std::collections::VecDeque;

use mmap_fifo::{MmapFifo, PAGE_EXTENSION, PAGE_PREFIX};
use rand::{RngExt, random};
use tempfile::tempdir;

#[test]
fn test_load_nonexistent_dir() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("does_not_exist");

    let err = MmapFifo::<u32>::load(&missing, 1024);
    assert!(err.is_err());
    assert!(err.is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound));
}

#[test]
fn test_new_creates_directory() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested").join("queue");

    assert!(!path.exists());

    let fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();
    assert!(path.exists());
    assert!(path.is_dir());
    assert!(fifo.is_empty());
}

#[test]
fn test_push_pop() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<String>::new(dir.path(), 1024).unwrap();

    assert!(fifo.is_empty());
    assert_eq!(fifo.len(), 0);

    fifo.push(&"hello".to_string()).unwrap();
    assert!(!fifo.is_empty());
    assert_eq!(fifo.len(), 1);

    fifo.push(&"world".to_string()).unwrap();
    assert_eq!(fifo.len(), 2);

    assert_eq!(fifo.pop().unwrap(), Some("hello".to_string()));
    assert_eq!(fifo.len(), 1);

    assert_eq!(fifo.pop().unwrap(), Some("world".to_string()));
    assert_eq!(fifo.len(), 0);
    assert!(fifo.is_empty());

    assert_eq!(fifo.pop().unwrap(), None);
}

#[test]
fn test_many_items() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();

    for i in 0..1000 {
        fifo.push(&i).unwrap();
        assert_eq!(fifo.len(), (i + 1) as usize);
    }

    for i in 0..1000 {
        assert_eq!(fifo.len(), (1000 - i) as usize);
        assert_eq!(fifo.pop().unwrap(), Some(i));
    }
    assert_eq!(fifo.len(), 0);
    assert!(fifo.is_empty());
    assert_eq!(fifo.pop().unwrap(), None);
}

#[test]
fn test_iter() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();

    for i in 0..5 {
        fifo.push(&i).unwrap();
    }

    // Test non-consuming iteration
    let mut count = 0;
    for (i, item) in fifo.iter().enumerate() {
        assert_eq!(item.unwrap(), i as u32);
        count += 1;
    }
    assert_eq!(count, 5);

    // Queue should still have items
    assert_eq!(fifo.len(), 5);

    // Pop some and test iter again
    fifo.pop().unwrap();
    fifo.pop().unwrap();

    let mut count = 0;
    for (i, item) in fifo.iter().enumerate() {
        assert_eq!(item.unwrap(), (i + 2) as u32);
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn test_into_iter() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();

    for i in 0..5 {
        fifo.push(&i).unwrap();
    }

    // Test consuming iteration
    let mut count = 0;
    for (i, item) in fifo.into_iter().enumerate() {
        assert_eq!(item.unwrap(), i as u32);
        count += 1;
    }
    assert_eq!(count, 5);

    // Cannot use fifo anymore as it was moved
}

#[test]
fn test_iter_with_rotation() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<u64>::new(dir.path(), 1024).unwrap();

    // 100 * 12 bytes = 1200 bytes, which forces rotation (page size 1024)
    for i in 0..100 {
        fifo.push(&(i as u64)).unwrap();
    }

    let mut count = 0;
    for (i, item) in fifo.iter().enumerate() {
        assert_eq!(item.unwrap(), i as u64);
        count += 1;
    }
    assert_eq!(count, 100);

    // Pop half and check iter again
    for _ in 0..50 {
        fifo.pop().unwrap();
    }

    let mut count = 0;
    for (i, item) in fifo.iter().enumerate() {
        assert_eq!(item.unwrap(), (i + 50) as u64);
        count += 1;
    }
    assert_eq!(count, 50);
}

#[test]
fn test_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        for i in 0..10 {
            fifo.push(&i).unwrap();
        }
        assert_eq!(fifo.len(), 10);
        // Don't drop it yet, let it be dropped at the end of scope
    }

    // Load it back
    {
        let mut fifo = MmapFifo::<u32>::load(&path, page_size).unwrap();
        assert_eq!(fifo.len(), 10);
        for i in 0..10 {
            assert_eq!(fifo.pop().unwrap(), Some(i));
        }
        assert_eq!(fifo.pop().unwrap(), None);
    }
}

#[test]
fn test_load_with_rotation() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024; // Use minimum page size

    {
        let mut fifo = MmapFifo::<u64>::new(&path, page_size).unwrap();
        // Each u64 is 8 bytes + 4 bytes len = 12 bytes.
        // 1024 / 12 = 85.33. So 100 items will force rotation.
        for i in 0..200 {
            fifo.push(&i).unwrap();
        }
        // Pop some to have read_pos not at the beginning
        for i in 0..50 {
            assert_eq!(fifo.pop().unwrap(), Some(i));
        }
        assert_eq!(fifo.len(), 150);
    }

    // Load it back
    {
        let mut fifo = MmapFifo::<u64>::load(&path, page_size).unwrap();
        assert_eq!(fifo.len(), 150);
        for i in 50..200 {
            assert_eq!(fifo.pop().unwrap(), Some(i));
        }
        assert_eq!(fifo.pop().unwrap(), None);

        // Should be able to push more
        for i in 200..300 {
            fifo.push(&i).unwrap();
        }
        assert_eq!(fifo.len(), 100);
        for i in 200..300 {
            assert_eq!(fifo.pop().unwrap(), Some(i));
        }
    }
}

#[test]
fn test_load_empty_dir() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    // Directory exists but is empty
    assert!(path.exists());

    // Load should succeed and create a fresh queue
    let mut fifo = MmapFifo::<u32>::load(&path, page_size).unwrap();
    assert_eq!(fifo.len(), 0);
    assert!(fifo.is_empty());

    // We should be able to push and pop
    fifo.push(&42).unwrap();
    assert_eq!(fifo.len(), 1);
    assert_eq!(fifo.pop().unwrap(), Some(42));
    assert_eq!(fifo.pop().unwrap(), None);
}

#[test]
fn test_clear() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();

    for i in 0..10 {
        fifo.push(&i).unwrap();
    }
    assert_eq!(fifo.len(), 10);

    fifo.clear().unwrap();

    assert_eq!(fifo.len(), 0);
    assert!(fifo.is_empty());
    assert_eq!(fifo.pop().unwrap(), None);

    // Re-use the queue
    for i in 0..5 {
        fifo.push(&(i + 10)).unwrap();
    }
    assert_eq!(fifo.len(), 5);
    assert!(!fifo.is_empty());
    for i in 0..5 {
        assert_eq!(fifo.pop().unwrap(), Some(i + 10));
    }
    assert_eq!(fifo.len(), 0);
    assert!(fifo.is_empty());
}

#[test]
fn test_invalid_page_size() {
    let dir = tempdir().unwrap();
    let res = MmapFifo::<u32>::new(dir.path(), 1023);
    assert!(res.is_err());
    assert_eq!(res.err().unwrap().kind(), std::io::ErrorKind::InvalidInput);

    let res_load = MmapFifo::<u32>::load(dir.path(), 512);
    assert!(res_load.is_err());
    assert_eq!(res_load.err().unwrap().kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn test_load_partial_pop() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        fifo.push(&1).unwrap();
        fifo.push(&2).unwrap();
        fifo.push(&3).unwrap();

        assert_eq!(fifo.pop().unwrap(), Some(1));
        // fifo.read_pos points to item 2
        // item 1 marked as popped in file
    }

    // Load back
    let mut loaded = MmapFifo::<u32>::load(&path, page_size).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded.pop().unwrap(), Some(2));
    assert_eq!(loaded.pop().unwrap(), Some(3));
    assert_eq!(loaded.pop().unwrap(), None);
}

#[test]
fn test_load_all_popped() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        fifo.push(&1).unwrap();
        fifo.push(&2).unwrap();
        assert_eq!(fifo.pop().unwrap(), Some(1));
        assert_eq!(fifo.pop().unwrap(), Some(2));
        assert_eq!(fifo.len(), 0);
        // Both items marked as popped, but page not deleted because it's the only page
    }

    // Load back
    let mut loaded = MmapFifo::<u32>::load(&path, page_size).unwrap();
    assert_eq!(loaded.len(), 0);
    assert!(loaded.is_empty());

    // Push new item
    loaded.push(&3).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded.pop().unwrap(), Some(3));
}

#[test]
fn test_clear_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        fifo.push(&1).unwrap();
        fifo.clear().unwrap();
    }

    // Load back
    let loaded = MmapFifo::<u32>::load(&path, page_size).unwrap();
    assert_eq!(loaded.len(), 0);
    assert!(loaded.is_empty());
}

#[test]
fn test_ignore_unrelated_files() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    if !path.exists() {
        std::fs::create_dir_all(&path).unwrap();
    }

    // Create unrelated files
    std::fs::write(path.join("other.txt"), "hello").unwrap();
    std::fs::write(path.join("page_not_mmap.tmp"), "content").unwrap();
    std::fs::write(path.join("not_page_1.mmap"), "content").unwrap();

    {
        let mut fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();
        fifo.push(&1).unwrap();
    }

    // Verify they still exist after new() (it should only clean page_*.mmap)
    assert!(path.join("other.txt").exists());
    assert!(path.join("page_not_mmap.tmp").exists());
    assert!(path.join("not_page_1.mmap").exists());

    // Load and verify
    let loaded = MmapFifo::<u32>::load(&path, 1024).unwrap();
    assert_eq!(loaded.len(), 1);

    assert!(path.join("other.txt").exists());
}

#[test]
fn test_iterator_empty() {
    let dir = tempdir().unwrap();
    let fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();

    // iter() on fresh
    assert_eq!(fifo.iter().count(), 0);

    // into_iter() on fresh
    assert_eq!(fifo.into_iter().count(), 0);

    let mut fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();
    fifo.push(&1).unwrap();
    fifo.pop().unwrap();

    // iter() after all popped
    assert_eq!(fifo.iter().count(), 0);
    // into_iter() after all popped
    assert_eq!(fifo.into_iter().count(), 0);
}

#[test]
fn test_repeated_clear_reuse() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();

    for cycle in 0..3 {
        fifo.push(&cycle).unwrap();
        fifo.push(&(cycle + 10)).unwrap();
        assert_eq!(fifo.pop().unwrap(), Some(cycle));
        fifo.clear().unwrap();
        assert_eq!(fifo.len(), 0);
        assert_eq!(fifo.pop().unwrap(), None);

        // Re-verify on disk via load
        let loaded = MmapFifo::<u32>::load(&path, 1024).unwrap();
        assert_eq!(loaded.len(), 0);
    }

    fifo.push(&100).unwrap();
    assert_eq!(fifo.pop().unwrap(), Some(100));
}

#[test]
fn test_corrupted_payload() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<String>::new(&path, page_size).unwrap();
        fifo.push(&"valid".to_string()).unwrap();
        // Offset for "valid" is 0.
        // Header: 4 bytes. Data: postcard String "valid" is 1 byte len + 5 bytes data = 6 bytes.
        // Total: 10 bytes.
    }

    // Corrupt the payload bytes but keep the header valid
    let p0 = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
    let mut data = std::fs::read(&p0).unwrap();
    // Postcard string: [len, ...bytes]
    // "valid" is [5, 'v', 'a', 'l', 'i', 'd']
    // We change the length byte to something impossible or just corrupt bytes
    data[4] = 255; // Impossible length for 1024 page if it was a varint, but here it's just wrong
    std::fs::write(&p0, data).unwrap();

    let mut loaded = MmapFifo::<String>::load(&path, page_size).unwrap();
    assert_eq!(loaded.len(), 1); // Structural validation succeeds

    // pop() should fail
    let res = loaded.pop();
    assert!(res.is_err());

    // iter() should fail
    let loaded2 = MmapFifo::<String>::load(&path, page_size).unwrap();
    let mut it = loaded2.iter();
    assert!(it.next().unwrap().is_err());

    // into_iter() should fail
    let loaded3 = MmapFifo::<String>::load(&path, page_size).unwrap();
    let mut into_it = loaded3.into_iter();
    assert!(into_it.next().unwrap().is_err());
}

#[test]
fn test_new_cleanup_nested_directories() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Create a directory that looks like a page file
    // Since id=0, it will be "page_0.mmap"
    let nested_dir = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
    std::fs::create_dir_all(&nested_dir).unwrap();
    std::fs::write(nested_dir.join("inside.txt"), "content").unwrap();

    // Calling new() should try to create page_0.mmap as a file.
    // If it's a directory, OpenOptions::open will fail.
    let result = MmapFifo::<u32>::new(&path, 1024);

    assert!(result.is_err());
    // In Linux, opening a directory with write access returns EISDIR (code 21)
    assert_eq!(result.err().unwrap().raw_os_error(), Some(21));

    // Verify the directory was NOT deleted by the cleanup logic
    assert!(nested_dir.exists());
    assert!(nested_dir.is_dir());
}

#[test]
fn test_clear_disk_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();

    fifo.push(&1).unwrap();
    fifo.push(&2).unwrap();
    // page_0.mmap exists

    fifo.clear().unwrap();

    // Assert disk state
    let entries: Vec<_> = std::fs::read_dir(&path).unwrap().collect();
    // Should only have page_0.mmap (freshly re-initialized)
    assert_eq!(entries.len(), 1);
    let entry = entries[0].as_ref().unwrap();
    assert_eq!(
        entry.file_name(),
        format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION).as_str()
    );

    let metadata = entry.metadata().unwrap();
    assert_eq!(metadata.len(), 1024);

    // Verify it's actually empty (all zeros)
    let data = std::fs::read(entry.path()).unwrap();
    assert!(data.iter().all(|&b| b == 0));
}

#[test]
fn test_load_wrong_page_size() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    // Create a page file with the wrong size
    let page_path = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
    std::fs::write(&page_path, vec![0u8; 512]).unwrap();

    let res = MmapFifo::<u32>::load(&path, page_size);
    assert!(res.is_err());
    assert_eq!(res.err().unwrap().kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn test_load_corrupted_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        fifo.push(&1).unwrap();
    }

    // Corrupt the length prefix in page_0.mmap
    let page_path = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
    let mut data = std::fs::read(&page_path).unwrap();
    // Item 1: len=4 (le bytes [4, 0, 0, 0]).
    // Let's set it to something that exceeds the page size.
    data[0] = 0xFF;
    data[1] = 0xFF;
    std::fs::write(&page_path, data).unwrap();

    let fifo = MmapFifo::<u32>::load(&path, page_size).unwrap();
    // restore_state should see the invalid len and stop scanning,
    // potentially marking the queue as empty or with fewer items.
    // In this case, since the first item is "corrupted", it might think it's empty.
    assert_eq!(fifo.len(), 0);
}

#[test]
fn test_pop_error_does_not_advance_state() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<String>::new(&path, page_size).unwrap();
        fifo.push(&"Item 1".to_string()).unwrap();
        fifo.push(&"Item 2".to_string()).unwrap();
        assert_eq!(fifo.len(), 2);
    }

    // Corrupt first item's payload in page_0.mmap
    let page_path = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
    let mut data = std::fs::read(&page_path).unwrap();

    // Item 1: len=6 (postcard String len prefix (1) + 6 chars)
    // [6, 73, 116, 101, 109, 32, 49]
    // Header: 4 bytes [6, 0, 0, 0]
    // Payload starts at 4.
    // Let's corrupt the payload bytes but keep the header intact.
    // Postcard deserialization will fail if payload bytes are invalid for the type.
    // For a String, we can make it invalid UTF-8.
    data[4] = 0xFF;
    std::fs::write(&page_path, data).unwrap();

    let mut fifo = MmapFifo::<String>::load(&path, page_size).unwrap();
    assert_eq!(fifo.len(), 2);

    // pop() -> Err
    let res = fifo.pop();
    assert!(res.is_err());

    // Reload
    drop(fifo);
    let mut loaded = MmapFifo::<String>::load(&path, page_size).unwrap();

    // pop() -> still Err, second item not silently reachable
    assert_eq!(loaded.len(), 2);
    let res2 = loaded.pop();
    assert!(res2.is_err());
    assert_eq!(loaded.len(), 2); // len should still be 2 because pop failed
}

#[test]
fn test_into_iter_partial_consumption_persists() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        fifo.push(&1).unwrap();
        fifo.push(&2).unwrap();
        fifo.push(&3).unwrap();
        fifo.push(&4).unwrap();

        let mut it = fifo.into_iter();
        assert_eq!(it.next().unwrap().unwrap(), 1);
        assert_eq!(it.next().unwrap().unwrap(), 2);
        // Drop iterator here
    }

    let mut loaded = MmapFifo::<u32>::load(&path, page_size).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded.pop().unwrap(), Some(3));
    assert_eq!(loaded.pop().unwrap(), Some(4));
    assert_eq!(loaded.pop().unwrap(), None);
}

#[test]
fn test_iter_after_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    {
        let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
        for i in 0..150 {
            fifo.push(&i).unwrap();
        }
        // Pop 140 items (all of page 0 and some of page 1)
        for _ in 0..140 {
            fifo.pop().unwrap();
        }
        // 10 items left in page 1.
    }

    let loaded = MmapFifo::<u32>::load(&path, page_size).unwrap();
    assert_eq!(loaded.len(), 10);

    let items: Vec<u32> = loaded.iter().map(|r| r.unwrap()).collect();
    let expected: Vec<u32> = (140..150).collect();
    assert_eq!(items, expected);
}

#[test]
fn test_lifecycle_reuse_without_clear() {
    let dir = tempdir().unwrap();
    let mut fifo = MmapFifo::<u32>::new(dir.path(), 1024).unwrap();

    // Cycle 1
    for i in 0..50 {
        fifo.push(&i).unwrap();
    }
    for i in 0..50 {
        assert_eq!(fifo.pop().unwrap(), Some(i));
    }
    assert!(fifo.is_empty());

    // Cycle 2: Same page reuse
    for i in 50..100 {
        fifo.push(&i).unwrap();
    }
    for i in 50..100 {
        assert_eq!(fifo.pop().unwrap(), Some(i));
    }
    assert!(fifo.is_empty());

    // Cycle 3: Force rotation
    for i in 100..300 {
        fifo.push(&i).unwrap();
    }
    for i in 100..300 {
        assert_eq!(fifo.pop().unwrap(), Some(i));
    }
    assert!(fifo.is_empty());

    // Cycle 4: Push after rotation and empty
    fifo.push(&1000).unwrap();
    assert_eq!(fifo.pop().unwrap(), Some(1000));
}

#[test]
fn test_randomized_model_comparison() {
    use rand::{SeedableRng, rngs::StdRng};

    let seed = std::env::var("MMAP_FIFO_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(random);

    println!("Running test_randomized_model_comparison with seed: {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let page_size = 1024;

    let mut model = VecDeque::new();
    let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();

    for i in 0..20000 {
        // Choose operation: 0-44: push, 45-84: pop, 85-89: iter, 90-94: reload, 95-99: clear
        let op = rng.random_range(0..100);

        if op < 45 {
            // push
            let val: u32 = rng.random();
            fifo.push(&val)
                .unwrap_or_else(|_| panic!("push failed at iteration {} with seed {}", i, seed));
            model.push_back(val);
        } else if op < 85 {
            // pop
            let fifo_val = fifo
                .pop()
                .unwrap_or_else(|_| panic!("pop failed at iteration {} with seed {}", i, seed));
            let model_val = model.pop_front();
            assert_eq!(fifo_val, model_val, "Mismatch at iteration {} with seed {}", i, seed);
        } else if op < 90 {
            // iter collect
            let fifo_items: Vec<u32> = fifo
                .iter()
                .map(|r| r.unwrap_or_else(|_| panic!("iter item failed at iteration {} with seed {}", i, seed)))
                .collect();
            let model_items: Vec<u32> = model.iter().copied().collect();
            assert_eq!(
                fifo_items, model_items,
                "Iterator mismatch at iteration {} with seed {}",
                i, seed
            );
        } else if op < 95 {
            // occasionally drop + load
            drop(fifo);
            fifo = MmapFifo::<u32>::load(&path, page_size)
                .unwrap_or_else(|_| panic!("load failed at iteration {} with seed {}", i, seed));
            assert_eq!(
                fifo.len(),
                model.len(),
                "Len mismatch after load at iteration {} with seed {}",
                i,
                seed
            );

            // Verify content after load via iterator
            let fifo_items: Vec<u32> = fifo
                .iter()
                .map(|r| r.unwrap_or_else(|_| panic!("iter after load failed at iteration {} with seed {}", i, seed)))
                .collect();
            let model_items: Vec<u32> = model.iter().copied().collect();
            assert_eq!(
                fifo_items, model_items,
                "Content mismatch after load at iteration {} with seed {}",
                i, seed
            );
        } else {
            // clear
            fifo.clear()
                .unwrap_or_else(|_| panic!("clear failed at iteration {} with seed {}", i, seed));
            model.clear();
            assert_eq!(
                fifo.len(),
                0,
                "Len not zero after clear at iteration {} with seed {}",
                i,
                seed
            );
            assert!(
                fifo.is_empty(),
                "Not empty after clear at iteration {} with seed {}",
                i,
                seed
            );
        }

        assert_eq!(
            fifo.len(),
            model.len(),
            "Len mismatch at end of iteration {} with seed {}",
            i,
            seed
        );
    }
}

#[test]
fn test_visit() -> std::io::Result<()> {
    let dir = tempdir()?;
    let mut fifo: MmapFifo<u32> = MmapFifo::new(dir.path(), 1024)?;

    fifo.push(&1)?;
    fifo.push(&2)?;
    fifo.push(&3)?;

    // Visit without changes
    let mut seen = Vec::new();
    fifo.visit(|&item| {
        seen.push(item);
        None
    })?;
    assert_eq!(seen, vec![1, 2, 3]);

    // Visit with changes (replacing 2 with 20)
    fifo.visit(|&item| if item == 2 { Some(20) } else { None })?;

    // Verify changes via pop
    assert_eq!(fifo.pop()?, Some(1));
    assert_eq!(fifo.pop()?, Some(20));
    assert_eq!(fifo.pop()?, Some(3));
    assert_eq!(fifo.pop()?, None);

    Ok(())
}

#[test]
fn test_visit_size_mismatch() -> std::io::Result<()> {
    let dir = tempdir()?;
    let mut fifo: MmapFifo<String> = MmapFifo::new(dir.path(), 1024)?;

    fifo.push(&"hello".to_string())?;

    // Replacing "hello" (5 bytes) with "world!" (6 bytes) should fail
    let result = fifo.visit(|s| if s == "hello" { Some("world!".to_string()) } else { None });

    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        assert!(e.to_string().contains("size mismatch"));
    }

    Ok(())
}

#[test]
fn test_visit_fifo_order() -> std::io::Result<()> {
    let dir = tempdir()?;
    let mut fifo: MmapFifo<u32> = MmapFifo::new(dir.path(), 1024)?;

    // Push 100 items
    for i in 0..100 {
        fifo.push(&i)?;
    }

    // Pop 10 items to move read_pos
    for i in 0..10 {
        assert_eq!(fifo.pop()?, Some(i));
    }

    // Push another 100 items (this will definitely span multiple pages)
    for i in 100..200 {
        fifo.push(&i)?;
    }

    // Visit remaining items (10..200)
    let mut seen = Vec::new();
    fifo.visit(|&item| {
        seen.push(item);
        None
    })?;

    let expected: Vec<u32> = (10..200).collect();
    assert_eq!(seen, expected, "Visit did not follow FIFO order");

    Ok(())
}

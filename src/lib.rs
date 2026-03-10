//! A memory-mapped file-backed FIFO queue library.
//!
//! This crate provides [`MmapFifo`], a FIFO queue that stores elements in memory-mapped files.
//! It is designed for cases where you need a persistent or large-scale queue that doesn't fit
//! entirely in RAM, while still benefiting from memory-mapped I/O performance.
//!
//! # Features
//! - **Fixed-size pages**: The queue is composed of multiple files (pages) of a configurable size (minimum 1024 bytes).
//! - **Automatic rotation**: New pages are created as needed when pushing items.
//! - **Automatic cleanup**: Pages are deleted from the disk once all items in them have been popped.
//! - **Serialization**: Uses `serde` and `postcard` for efficient element serialization.
//!
//! # Examples
//!
//! ```
//! use mmap_fifo::MmapFifo;
//! use tempfile::tempdir;
//!
//! # fn main() -> std::io::Result<()> {
//! let dir = tempdir()?;
//! let mut fifo = MmapFifo::<String>::new(dir.path(), 1024)?;
//!
//! fifo.push(&"First item".to_string())?;
//! fifo.push(&"Second item".to_string())?;
//!
//! drop(fifo);
//!
//! // Restore the state
//! let mut restored_fifo = MmapFifo::<String>::load(dir.path(), 1024)?;
//! assert_eq!(restored_fifo.len(), 2);
//! assert_eq!(restored_fifo.pop()?, Some("First item".to_string()));
//! assert_eq!(restored_fifo.pop()?, Some("Second item".to_string()));
//! assert_eq!(restored_fifo.pop()?, None);
//!
//! // Iterate without popping
//! restored_fifo.push(&"Third item".to_string())?;
//! let items: Vec<_> = restored_fifo.iter().map(|r| r.unwrap()).collect();
//! assert_eq!(items, vec!["Third item".to_string()]);
//!
//! // Consuming iteration
//! let items: Vec<_> = restored_fifo.into_iter().map(|r| r.unwrap()).collect();
//! assert_eq!(items, vec!["Third item".to_string()]);
//! # Ok(())
//! # }
//! ```

use std::{
    collections::VecDeque,
    fs::OpenOptions,
    marker::PhantomData,
    path::{Path, PathBuf},
};

use memmap2::{MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};

/// Prefix for the names of memory-mapped page files.
pub const PAGE_PREFIX: &str = "mmfifo_pg_";
/// File name extension of memory-mapped page files.
pub const PAGE_EXTENSION: &str = ".mmap";

/// A memory-mapped file-backed FIFO queue for elements that can be serialized/deserialized.
///
/// The queue is composed of fixed-size pages, each being a memory-mapped file on disk.
/// As items are pushed, new pages are created when the current one is full.
/// As items are popped and a page becomes empty, it is automatically deleted from the disk.
///
/// # Type Parameters
/// * `T`: The type of elements stored in the queue. Must implement `serde::Serialize` and `serde::Deserialize`.
#[derive(Debug)]
pub struct MmapFifo<T> {
    base_path: PathBuf,
    page_size: usize,
    pages: VecDeque<MmapPage>,
    read_pos: PageOffset,
    write_pos: PageOffset,
    len: usize,
    _marker: PhantomData<T>,
}

#[derive(Debug)]
struct MmapPage {
    id: u64,
    mmap: MmapMut,
    path: PathBuf,
}

#[derive(Clone, Copy, Debug, Default)]
struct PageOffset {
    page_idx: usize, // index in the `pages` VecDeque
    offset: usize,
}

impl<T> MmapFifo<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Creates a new `MmapFifo` at the specified `base_path`.
    ///
    /// If the directory already contains page files, **all of them will be deleted**
    /// to ensure a clean state for the new queue.
    ///
    /// # Arguments
    /// * `base_path`: The directory where memory-mapped page files will be stored. If the directory does not exist, it
    ///   will be created.
    /// * `page_size`: The size in bytes of each memory-mapped page file.
    ///
    /// # Errors
    /// Returns an `std::io::Error` if the directory cannot be created, if existing
    /// page files cannot be deleted, or if the first page file cannot be initialized.
    /// Also returns `ErrorKind::InvalidInput` if `page_size` is less than 1024 bytes (the minimum allowed).
    pub fn new<P: AsRef<Path>>(base_path: P, page_size: usize) -> std::io::Result<Self> {
        if page_size < 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page_size must be at least 1024 bytes",
            ));
        }
        let base_path = base_path.as_ref().to_path_buf();
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path)?;
        } else {
            // Clean up existing page files
            for entry in std::fs::read_dir(&base_path)? {
                let entry = entry?;
                let path = entry.path();
                let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let is_page_file =
                    path.is_file() && file_name.starts_with(PAGE_PREFIX) && file_name.ends_with(PAGE_EXTENSION);

                if is_page_file {
                    std::fs::remove_file(path)?;
                }
            }
        }

        let mut fifo = Self {
            base_path,
            page_size,
            pages: VecDeque::new(),
            read_pos: PageOffset::default(),
            write_pos: PageOffset::default(),
            len: 0,
            _marker: PhantomData,
        };

        // Initialize with one page
        fifo.add_page()?;
        Ok(fifo)
    }

    fn add_page(&mut self) -> std::io::Result<()> {
        let id = if let Some(last) = self.pages.back() {
            last.id + 1
        } else {
            0
        };

        let path = self.base_path.join(format!("{}{}{}", PAGE_PREFIX, id, PAGE_EXTENSION));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(self.page_size as u64)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        self.pages.push_back(MmapPage { id, mmap, path });
        Ok(())
    }

    /// Pushes an item into the queue.
    ///
    /// The item is serialized using `postcard` and written to the current write page.
    /// If the item (including its 4-byte length prefix) exceeds the remaining space in the
    /// current page, a new page is automatically created.
    ///
    /// # Errors
    /// * Returns `std::io::ErrorKind::InvalidInput` if the item's serialized size exceeds the `page_size`.
    /// * Returns `std::io::Error` if there's an issue writing to the memory-mapped file or creating a new page.
    pub fn push(&mut self, item: &T) -> std::io::Result<()> {
        let bytes = postcard::to_stdvec(item).map_err(std::io::Error::other)?;
        let len = bytes.len() as u32;
        let total_size = 4 + bytes.len();

        if total_size > self.page_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Item too large for page size",
            ));
        }

        if len & 0x8000_0000 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Item serialized size exceeds supported limit (2GB)",
            ));
        }

        // Check if we need a new page
        if self.write_pos.offset + total_size > self.page_size {
            self.add_page()?;
            self.write_pos.page_idx = self.pages.len() - 1;
            self.write_pos.offset = 0;
        }

        let page = &mut self.pages[self.write_pos.page_idx];
        let offset = self.write_pos.offset;

        // Write length
        page.mmap[offset..offset + 4].copy_from_slice(&len.to_le_bytes());
        // Write data
        page.mmap[offset + 4..offset + total_size].copy_from_slice(&bytes);
        page.mmap
            .flush_range(offset, total_size)
            .map_err(std::io::Error::other)?;

        self.write_pos.offset += total_size;
        self.len += 1;
        Ok(())
    }

    fn drop_front_page(&mut self) {
        if let Some(page) = self.pages.pop_front() {
            let path = page.path.clone();
            drop(page);
            let _ = std::fs::remove_file(path);

            // Adjust indices
            if self.write_pos.page_idx > 0 {
                self.write_pos.page_idx -= 1;
            }
            // read_pos.page_idx is always 0 because we always pop the front page that we are currently reading from.
            self.read_pos.page_idx = 0;
            self.read_pos.offset = 0;
        }
    }

    /// Pops an item from the queue.
    ///
    /// Returns `Ok(Some(item))` if an item is available, or `Ok(None)` if the queue is empty.
    /// When all items in the oldest page have been popped, the page file is automatically
    /// deleted from the disk.
    ///
    /// # Errors
    /// Returns `std::io::Error` if there's an issue reading from the memory-mapped file
    /// or deserializing the item.
    pub fn pop(&mut self) -> std::io::Result<Option<T>> {
        loop {
            // Check if queue is empty: read and write positions are the same
            if self.read_pos.page_idx == self.write_pos.page_idx && self.read_pos.offset == self.write_pos.offset {
                return Ok(None);
            }

            let page_idx = self.read_pos.page_idx;
            let offset = self.read_pos.offset;

            // Check if the current page is finished: 4 bytes for length prefix must fit
            if offset + 4 > self.page_size {
                if self.pages.len() > 1 {
                    // Page exhausted, delete it and move to next
                    self.drop_front_page();
                    continue;
                } else {
                    return Ok(None);
                }
            }

            let page = &self.pages[page_idx];
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&page.mmap[offset..offset + 4]);
            let len = u32::from_le_bytes(len_bytes) as usize;

            // len == 0 indicates no more items in this page
            if len == 0 {
                if self.pages.len() > 1 {
                    // Page exhausted, delete it and move to next
                    self.drop_front_page();
                    continue;
                } else {
                    return Ok(None);
                }
            }

            let item_total_size = 4 + len;
            // Check if the full item (prefix + data) fits in the remaining page space
            if offset + item_total_size > self.page_size {
                if self.pages.len() > 1 {
                    // Page exhausted, delete it and move to the next
                    self.drop_front_page();
                    continue;
                } else {
                    return Ok(None);
                }
            }

            // Deserialize data
            let data = &page.mmap[offset + 4..offset + item_total_size];
            let item: T = postcard::from_bytes(data).map_err(std::io::Error::other)?;

            // Mark as popped by setting high bit of length (allows persistence to track state)
            let page_mut = &mut self.pages[page_idx];
            let marked_len = (len as u32) | 0x8000_0000;
            page_mut.mmap[offset..offset + 4].copy_from_slice(&marked_len.to_le_bytes());
            page_mut.mmap.flush_range(offset, 4).map_err(std::io::Error::other)?;

            // Update read position and length
            self.read_pos.offset += item_total_size;
            self.len -= 1;

            return Ok(Some(item));
        }
    }

    /// Returns the number of elements in the queue.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Visits each item in the queue in order.
    ///
    /// The closure `f` is called with a reference to the currently visited item.
    /// If the closure returns `Some(new_item)`, the visited item will be replaced by
    /// the new item in the queue's persistent storage.
    ///
    /// # Important
    /// The serialized size of the `new_item` **must be exactly the same** as the
    /// serialized size of the original item. If the size differs, this method returns
    /// an `std::io::Error`.
    ///
    /// # Errors
    /// Returns `std::io::Error` if:
    /// * There's an issue reading or writing to the memory-mapped files.
    /// * Deserialization of an existing item fails.
    /// * Serialization of the replacement item fails.
    /// * The replacement item's serialized size does not match the original.
    pub fn visit<F>(&mut self, mut f: F) -> std::io::Result<()>
    where
        F: FnMut(&T) -> Option<T>,
    {
        let mut pos = self.read_pos;

        loop {
            // Check if the traversal reached the end: the current traversal position matches the write position
            if pos.page_idx == self.write_pos.page_idx && pos.offset == self.write_pos.offset {
                break;
            }

            let page_idx = pos.page_idx;
            let offset = pos.offset;

            // Check if the current page is finished: 4 bytes for length prefix must fit
            if offset + 4 > self.page_size {
                if page_idx + 1 < self.pages.len() {
                    // Move to the next page
                    pos.page_idx += 1;
                    pos.offset = 0;
                    continue;
                } else {
                    break;
                }
            }

            let page = &self.pages[page_idx];
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&page.mmap[offset..offset + 4]);
            let raw_len = u32::from_le_bytes(len_bytes);

            // raw_len == 0 indicates no more items in this page
            if raw_len == 0 {
                if page_idx + 1 < self.pages.len() {
                    // Move to the next page
                    pos.page_idx += 1;
                    pos.offset = 0;
                    continue;
                } else {
                    break;
                }
            }

            // High bit set means item has been popped (we skip it during traversal)
            let is_popped = (raw_len & 0x8000_0000) != 0;
            let len = (raw_len & 0x7FFF_FFFF) as usize;
            let item_total_size = 4 + len;

            // Check if the full item fits in the remaining page space
            if offset + item_total_size > self.page_size {
                if page_idx + 1 < self.pages.len() {
                    // Move to the next page
                    pos.page_idx += 1;
                    pos.offset = 0;
                    continue;
                } else {
                    break;
                }
            }

            // Update position for next iteration
            pos.offset += item_total_size;

            if is_popped {
                // If it was already popped, just skip it
                continue;
            }

            // Deserialize data
            let data = &page.mmap[offset + 4..offset + item_total_size];
            let item: T = postcard::from_bytes(data).map_err(std::io::Error::other)?;

            // Call closure
            if let Some(new_item) = f(&item) {
                // Serialize new item
                let new_bytes = postcard::to_stdvec(&new_item).map_err(std::io::Error::other)?;
                if new_bytes.len() != len {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Replacement item serialized size mismatch: expected {} bytes, got {} bytes",
                            len,
                            new_bytes.len()
                        ),
                    ));
                }

                // Write new item to mmap
                let page_mut = &mut self.pages[page_idx];
                page_mut.mmap[offset + 4..offset + item_total_size].copy_from_slice(&new_bytes);
                page_mut
                    .mmap
                    .flush_range(offset + 4, len)
                    .map_err(std::io::Error::other)?;
            }
        }

        Ok(())
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Deletes all page files associated with this queue from the disk and reinitializes the queue.
    pub fn clear(&mut self) -> std::io::Result<()> {
        while let Some(page) = self.pages.pop_front() {
            let path = page.path.clone();
            drop(page);
            let _ = std::fs::remove_file(path);
        }
        self.len = 0;
        self.read_pos = PageOffset::default();
        self.write_pos = PageOffset::default();

        // Re-initialize with one page to make it immediately re-usable
        self.add_page()?;
        Ok(())
    }

    /// Loads an existing `MmapFifo` from the specified `base_path`.
    ///
    /// It scans the directory for existing page files and restores the queue's state,
    /// including the read and write positions and the total number of elements.
    ///
    /// If the directory is empty or does not contain any valid page files, it will
    /// initialize a new `MmapFifo` in that directory (equivalent to calling [`MmapFifo::new`]).
    ///
    /// # Arguments
    /// * `base_path`: The directory where existing memory-mapped page files are stored.
    /// * `page_size`: The size in bytes of each memory-mapped page file. This must match the size used when the queue
    ///   was originally created.
    ///
    /// # Errors
    /// Returns an `std::io::Error` if:
    /// * The `base_path` does not exist.
    /// * Page files cannot be opened or memory-mapped.
    /// * The directory contains files with unexpected formats or sizes.
    /// * `page_size` is less than 1024 bytes (the minimum allowed).
    pub fn load<P: AsRef<Path>>(base_path: P, page_size: usize) -> std::io::Result<Self> {
        if page_size < 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page_size must be at least 1024 bytes",
            ));
        }
        let base_path = base_path.as_ref().to_path_buf();
        if !base_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "base_path does not exist",
            ));
        }

        // Scan directory for page files: "page_<id>.mmap"
        let mut page_files = Vec::new();
        for entry in std::fs::read_dir(&base_path)? {
            let entry = entry?;
            let path = entry.path();
            let id = path
                .file_name()
                .and_then(|n| n.to_str())
                // Ensure filename matches prefix and extension
                .filter(|name| path.is_file() && name.starts_with(PAGE_PREFIX) && name.ends_with(PAGE_EXTENSION))
                // Extract and parse numeric ID
                .and_then(|name| {
                    name[PAGE_PREFIX.len()..name.len() - PAGE_EXTENSION.len()]
                        .parse::<u64>()
                        .ok()
                });

            if let Some(id) = id {
                page_files.push((id, path));
            }
        }

        // Sort by ID to ensure correct page sequence
        page_files.sort_by_key(|(id, _)| *id);

        // Check for continuous page ID sequence
        for i in 0..page_files.len().saturating_sub(1) {
            if page_files[i + 1].0 != page_files[i].0 + 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Missing page file in sequence between ID {} and {}",
                        page_files[i].0,
                        page_files[i + 1].0
                    ),
                ));
            }
        }

        // Open and memory-map each page file
        let mut pages = VecDeque::new();
        for (id, path) in page_files {
            let file = OpenOptions::new().read(true).write(true).open(&path)?;

            let metadata = file.metadata()?;
            if metadata.len() != page_size as u64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Page file {:?} has unexpected size: expected {}, found {}",
                        path,
                        page_size,
                        metadata.len()
                    ),
                ));
            }

            let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
            pages.push_back(MmapPage { id, mmap, path });
        }

        // If no pages found, fallback to initializing a new queue
        if pages.is_empty() {
            return Self::new(base_path, page_size);
        }

        let mut fifo = Self {
            base_path,
            page_size,
            pages,
            read_pos: PageOffset::default(),
            write_pos: PageOffset::default(),
            len: 0,
            _marker: PhantomData,
        };

        // Reconstruct read/write positions and count unpopped items
        fifo.restore_state()?;
        Ok(fifo)
    }

    fn restore_state(&mut self) -> std::io::Result<()> {
        let mut first_unpopped_found = false;
        let mut write_pos_found = false;
        let mut total_len = 0;

        // Default positions in case nothing is found
        self.read_pos = PageOffset { page_idx: 0, offset: 0 };
        self.write_pos = PageOffset { page_idx: 0, offset: 0 };

        for (page_idx, page) in self.pages.iter().enumerate() {
            let mut offset = 0;

            while offset + 4 <= self.page_size {
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&page.mmap[offset..offset + 4]);
                let raw_len = u32::from_le_bytes(len_bytes);

                if raw_len == 0 {
                    // End of items in this page
                    if !write_pos_found {
                        self.write_pos = PageOffset { page_idx, offset };
                        write_pos_found = true;
                    }
                    break;
                }

                let is_popped = (raw_len & 0x8000_0000) != 0;
                let item_len = (raw_len & 0x7FFF_FFFF) as usize;

                if offset + 4 + item_len > self.page_size {
                    // Corrupted or end of page space
                    if !write_pos_found {
                        self.write_pos = PageOffset { page_idx, offset };
                        write_pos_found = true;
                    }
                    break;
                }

                if !is_popped {
                    if !first_unpopped_found {
                        self.read_pos = PageOffset { page_idx, offset };
                        first_unpopped_found = true;
                    }
                    total_len += 1;
                }

                offset += 4 + item_len;

                // Exactly at the end of the page
                if offset == self.page_size {
                    // If we're at the end of this page and it's the last page, write_pos could be here
                    if !write_pos_found && page_idx == self.pages.len() - 1 {
                        self.write_pos = PageOffset {
                            page_idx,
                            offset: self.page_size,
                        };
                        write_pos_found = true;
                    }
                    // If we still haven't found unpopped, read_pos could be at the start of the next page
                    if !first_unpopped_found {
                        self.read_pos = PageOffset {
                            page_idx: page_idx + 1,
                            offset: 0,
                        };
                    }
                    break;
                }
            }
        }

        if !write_pos_found {
            // All pages are completely full with non-zero headers
            self.write_pos = PageOffset {
                page_idx: self.pages.len() - 1,
                offset: self.page_size,
            };
        }

        if !first_unpopped_found {
            // No unpopped items, read_pos should follow write_pos
            self.read_pos = self.write_pos;
            self.len = 0;
        } else {
            self.len = total_len;
        }

        Ok(())
    }

    /// Returns an iterator that yields elements of the queue without popping them.
    ///
    /// The iterator yields `std::io::Result<T>`.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            fifo: self,
            pos: self.read_pos,
        }
    }
}

impl<T> IntoIterator for MmapFifo<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    type IntoIter = IntoIter<T>;
    type Item = std::io::Result<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { fifo: self }
    }
}

/// An iterator over the elements of a [`MmapFifo`] that does not pop them.
///
/// This struct is created by the [`iter`](MmapFifo::iter) method.
pub struct Iter<'a, T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fifo: &'a MmapFifo<T>,
    pos: PageOffset,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    type Item = std::io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if the iterator reached the end: the current traversal position matches the write position
            if self.pos.page_idx == self.fifo.write_pos.page_idx && self.pos.offset == self.fifo.write_pos.offset {
                return None;
            }

            let page_idx = self.pos.page_idx;
            let offset = self.pos.offset;

            // Check if the current page is finished: 4 bytes for length prefix must fit
            if offset + 4 > self.fifo.page_size {
                if page_idx + 1 < self.fifo.pages.len() {
                    // Move to the next page
                    self.pos.page_idx += 1;
                    self.pos.offset = 0;
                    continue;
                } else {
                    return None;
                }
            }

            let page = &self.fifo.pages[page_idx];
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&page.mmap[offset..offset + 4]);
            let raw_len = u32::from_le_bytes(len_bytes);

            // raw_len == 0 indicates no more items in this page
            if raw_len == 0 {
                if page_idx + 1 < self.fifo.pages.len() {
                    // Move to the next page
                    self.pos.page_idx += 1;
                    self.pos.offset = 0;
                    continue;
                } else {
                    return None;
                }
            }

            // High bit set means item has been popped (we skip it during traversal)
            let is_popped = (raw_len & 0x8000_0000) != 0;
            let len = (raw_len & 0x7FFF_FFFF) as usize;
            let item_total_size = 4 + len;

            // Check if full item fits in remaining page space
            if offset + item_total_size > self.fifo.page_size {
                if page_idx + 1 < self.fifo.pages.len() {
                    // Move to the next page
                    self.pos.page_idx += 1;
                    self.pos.offset = 0;
                    continue;
                } else {
                    return None;
                }
            }

            // Update position for next iteration before returning a result
            self.pos.offset += item_total_size;

            if is_popped {
                // If it was already popped, just skip it
                continue;
            }

            // Deserialize data
            let data = &page.mmap[offset + 4..offset + item_total_size];
            return match postcard::from_bytes(data) {
                Ok(item) => Some(Ok(item)),
                Err(e) => Some(Err(std::io::Error::other(e))),
            };
        }
    }
}

/// An iterator that moves out of a [`MmapFifo`], popping items as it goes.
///
/// This struct is created by the [`into_iter`](MmapFifo::into_iter) method.
pub struct IntoIter<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fifo: MmapFifo<T>,
}

impl<T> Iterator for IntoIter<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    type Item = std::io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fifo.pop() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_visit_multi_page() -> std::io::Result<()> {
        let dir = tempdir()?;
        // Small page size to force multiple pages
        let page_size = 1024;
        // Use a type that has fixed serialized size in postcard
        // [u8; 1] is always 1 byte.
        let mut fifo: MmapFifo<[u8; 1]> = MmapFifo::new(dir.path(), page_size)?;

        // Each [u8; 1] takes 1 byte + 4 bytes length = 5 bytes.
        // 1024 / 5 = 204.8. 300 items will definitely use at least 2 pages.
        for i in 0..300 {
            fifo.push(&[(i % 255) as u8])?;
        }
        assert!(fifo.pages.len() > 1);

        // Visit and increase all items. Size remains 1 byte.
        fifo.visit(|&item| Some([(item[0].wrapping_add(1))]))?;

        // Verify items
        for i in 0..300 {
            assert_eq!(fifo.pop()?, Some([(i % 255) as u8 + 1]));
        }

        Ok(())
    }

    #[test]
    fn test_large_items() {
        let dir = tempdir().unwrap();
        let mut fifo = MmapFifo::<Vec<u8>>::new(dir.path(), 1024).unwrap();

        let item_512 = vec![0u8; 506];
        // 4 (header) + 506 (data) + 2 (postcard len overhead) = 512.
        fifo.push(&item_512).unwrap();
        assert_eq!(fifo.write_pos.offset, 512);

        let large_item = vec![0u8; 1000];
        // total_size = 4 + 1000 + postcard overhead.
        // Postcard Vec<u8> overhead is a varint for len.
        // For 1000, it's 2 bytes.
        // 4 + 1002 = 1006. Should fit in 1024.
        fifo.push(&large_item).unwrap();
        assert_eq!(fifo.len(), 2);
        assert_eq!(fifo.pop().unwrap(), Some(item_512));
        assert_eq!(fifo.pop().unwrap(), Some(large_item));
        assert_eq!(fifo.len(), 0);

        let too_large_item = vec![0u8; 1020];
        assert!(fifo.push(&too_large_item).is_err());
        assert_eq!(fifo.len(), 0);
    }

    #[test]
    fn test_new_on_existing_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create a queue with 2 pages
        {
            let mut fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();
            // u32 is 4 bytes + 4 bytes len = 8 bytes.
            // 200 * 8 = 1600 bytes, which forces at least 2 pages of 1024 bytes.
            for i in 0..200 {
                fifo.push(&i).unwrap();
            }
            assert!(fifo.pages.len() >= 2);
            // Drop it, files remain: page_0.mmap, page_1.mmap ...
        }

        // Call `new` on the same directory. This should delete existing pages.
        {
            let _fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();
        }

        // Now try to `load` from this directory. It should be empty because `new` cleared it.
        let loaded = MmapFifo::<u32>::load(&path, 1024).unwrap();
        assert_eq!(loaded.len(), 0);
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_page_boundary_edge_cases() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();

        // 1. Item exactly fills page space
        // Header: 4 bytes
        // Data: Vec<u8> overhead for length 1018 is 2 bytes.
        // total = 4 + 2 + 1018 = 1024.
        let payload_size = 1018;
        let item = vec![0u8; payload_size];
        fifo.push(&item).unwrap();
        assert_eq!(fifo.pages.len(), 1);
        assert_eq!(fifo.write_pos.offset, page_size);

        // Next push must trigger new page
        fifo.push(&vec![1u8]).unwrap();
        assert_eq!(fifo.pages.len(), 2);
        assert_eq!(fifo.write_pos.page_idx, 1);

        fifo.clear().unwrap();

        // 2. Leaves fewer than 4 bytes at end of page
        // item: header (4) + overhead (2) + payload (1016) = 1022.
        // 2 bytes left (< 4).
        let item = vec![0u8; 1016];
        fifo.push(&item).unwrap();
        assert_eq!(fifo.write_pos.offset, 1022);

        // Push should trigger new page because 4 bytes don't fit
        fifo.push(&vec![2u8]).unwrap();
        assert_eq!(fifo.pages.len(), 2);
        assert_eq!(fifo.write_pos.page_idx, 1);

        fifo.clear().unwrap();

        // 3. Exactly 4 bytes left (can fit header but not data)
        // item: header (4) + overhead (2) + payload (1014) = 1020.
        // 4 bytes left.
        let item = vec![0u8; 1014];
        fifo.push(&item).unwrap();
        assert_eq!(fifo.write_pos.offset, 1020);

        // Push should trigger new page because header (4) fits, but data doesn't
        fifo.push(&vec![3u8]).unwrap();
        assert_eq!(fifo.pages.len(), 2);

        // Verify all can be popped
        assert_eq!(fifo.pop().unwrap().map(|v| v.len()), Some(1014));
        assert_eq!(fifo.pop().unwrap(), Some(vec![3u8]));
    }

    #[test]
    fn test_load_first_page_fully_popped_then_second_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        {
            let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();
            // 1. Push items that fill the first page and go into the second.
            // Page size 1024.
            // 512 bytes each (incl header 4 + overhead 2 + 506 data = 512).
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            // Page 0 is now EXACTLY full. (512 + 512 = 1024).

            // 2. This must go to page 1.
            fifo.push(&vec![1u8; 100]).unwrap();

            assert_eq!(fifo.pages.len(), 2, "Should have 2 pages after 3 pushes");
            assert_eq!(fifo.write_pos.page_idx, 1);

            // 3. Pop first item from first page.
            assert_eq!(fifo.pop().unwrap().map(|v| v.len()), Some(506));
            // fifo.read_pos points to second item in page 0.
        }

        // Load back
        let mut loaded = MmapFifo::<Vec<u8>>::load(&path, page_size).unwrap();
        assert_eq!(loaded.len(), 2, "Should have 2 unpopped items");
        assert_eq!(loaded.pop().unwrap().map(|v| v.len()), Some(506));
        assert_eq!(loaded.pop().unwrap().map(|v| v.len()), Some(100));
        assert_eq!(loaded.pop().unwrap(), None);
    }

    #[test]
    fn test_page_cleanup_after_rotation() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;
        let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();

        // 1. Push items to fill page 0 and start page 1
        // (4 + 2 + 506 = 512). 2 items fill 1024 exactly.
        fifo.push(&vec![0u8; 506]).unwrap();
        fifo.push(&vec![0u8; 506]).unwrap();
        // Page 0 is full.
        fifo.push(&vec![1u8; 100]).unwrap(); // New page 1

        assert_eq!(fifo.pages.len(), 2);
        let p0 = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
        let p1 = path.join(format!("{}1{}", PAGE_PREFIX, PAGE_EXTENSION));
        assert!(p0.exists());
        assert!(p1.exists());

        // 2. Pop first 2 items (page 0 is exhausted)
        fifo.pop().unwrap();
        assert!(p0.exists(), "Page 0 should still exist until fully popped AND rotated");
        fifo.pop().unwrap();

        assert_eq!(fifo.len(), 1);
        assert!(p0.exists(), "Page 0 should still exist until pop() triggers rotation");

        // This pop should trigger rotation and deletion of page 0
        let item = fifo.pop().unwrap();
        assert_eq!(item.unwrap().len(), 100);
        assert!(!p0.exists(), "Page 0 should be deleted now");
        assert!(p1.exists(), "Page 1 should still exist");
    }

    #[test]
    fn test_persistence_after_front_page_dropped() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        {
            let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![1u8; 100]).unwrap(); // Page 1

            // Pop until page 0 is gone
            fifo.pop().unwrap();
            fifo.pop().unwrap();
            fifo.pop().unwrap(); // This pop() rotates page 0 out

            assert_eq!(fifo.pages[0].id, 1);
            let p0 = path.join(format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION));
            assert!(!p0.exists());
        }

        // Now load. It should start from page 1.
        let mut loaded = MmapFifo::<Vec<u8>>::load(&path, page_size).unwrap();
        assert_eq!(loaded.len(), 0);
        loaded.push(&vec![2u8; 50]).unwrap();
        assert_eq!(loaded.pop().unwrap(), Some(vec![2u8; 50]));
    }

    #[test]
    fn test_load_all_popped_multi_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        {
            let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![1u8; 506]).unwrap(); // Page 1
            fifo.push(&vec![1u8; 506]).unwrap(); // Page 1

            // Pop all items.
            // Page 0 should be deleted when we start popping from Page 1.
            // Page 1 should remain (it's the last page) but all items marked popped.
            for _ in 0..4 {
                fifo.pop().unwrap();
            }
            assert_eq!(fifo.len(), 0);
            assert_eq!(fifo.pages.len(), 1);
            assert_eq!(fifo.pages[0].id, 1);
        }

        // Reload
        let mut loaded = MmapFifo::<Vec<u8>>::load(&path, page_size).unwrap();
        assert_eq!(loaded.len(), 0);
        assert!(loaded.is_empty());

        // Reuse
        loaded.push(&vec![2u8; 100]).unwrap();
        assert_eq!(loaded.pop().unwrap(), Some(vec![2u8; 100]));
    }

    #[test]
    fn test_iter_boundary_with_skipped() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;
        let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();

        // Page 0: [Item 1 (512), Item 2 (512)]
        fifo.push(&vec![1u8; 506]).unwrap();
        fifo.push(&vec![2u8; 506]).unwrap();
        // Page 1: [Item 3 (512), Item 4 (512)]
        fifo.push(&vec![3u8; 506]).unwrap();
        fifo.push(&vec![4u8; 506]).unwrap();

        // Pop items 1, 2, 3
        fifo.pop().unwrap(); // Item 1
        fifo.pop().unwrap(); // Item 2
        fifo.pop().unwrap(); // Item 3 (this rotates out Page 0)

        // Now:
        // Page 1 exists, Item 3 is marked popped, Item 4 is unpopped.
        assert_eq!(fifo.pages.len(), 1);
        assert_eq!(fifo.pages[0].id, 1);
        assert_eq!(fifo.len(), 1);

        // Iterator should skip Item 3 and return Item 4
        let mut it = fifo.iter();
        let first = it.next().unwrap().unwrap();
        assert_eq!(first.len(), 506);
        assert_eq!(first[0], 4u8);
        assert!(it.next().is_none());
    }

    #[test]
    fn test_loader_filename_edge_cases() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // 1. Valid
        let mut fifo = MmapFifo::<u32>::new(&path, 1024).unwrap();
        fifo.push(&1).unwrap();
        drop(fifo);

        // 2. Edge cases (should be ignored)
        std::fs::write(path.join("page_.mmap"), "ignore").unwrap();
        std::fs::write(path.join("page_abc.mmap"), "ignore").unwrap();
        std::fs::write(path.join("page_1.mmap.bak"), "ignore").unwrap();
        std::fs::write(path.join("page_-1.mmap"), "ignore").unwrap();

        let loaded = MmapFifo::<u32>::load(&path, 1024).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.pages.len(), 1);
        assert_eq!(loaded.pages[0].id, 0);
    }

    #[test]
    fn test_persistence_exact_page_fill() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        {
            let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();
            // Exactly fill page 0. 2 items of 512.
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            assert_eq!(fifo.write_pos.offset, 1024);
            assert_eq!(fifo.pages.len(), 1);
        }

        {
            let mut loaded = MmapFifo::<Vec<u8>>::load(&path, page_size).unwrap();
            assert_eq!(loaded.len(), 2);
            assert_eq!(loaded.write_pos.offset, 1024);

            // Push another item, should trigger page 1
            loaded.push(&vec![1u8; 100]).unwrap();
            assert_eq!(loaded.pages.len(), 2);
            assert_eq!(loaded.pages[1].id, 1);

            assert_eq!(loaded.pop().unwrap().map(|v| v.len()), Some(506));
            assert_eq!(loaded.pop().unwrap().map(|v| v.len()), Some(506));
            assert_eq!(loaded.pop().unwrap().map(|v| v.len()), Some(100));
        }
    }

    #[test]
    fn test_load_missing_page_sequence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        // Create page_0.mmap and page_2.mmap, but skip page_1.mmap
        {
            let mut fifo = MmapFifo::<u32>::new(&path, page_size).unwrap();
            fifo.push(&1).unwrap(); // in page_0

            // Manually add page_2.mmap by tricking it or just creating files
            // Actually, we can just push enough to get to page 1, then rename it to 2.
            // But u32 is 8 bytes, 1024/8 = 128 items per page.
            for i in 0..300 {
                fifo.push(&i).unwrap();
            }
            // Should have 2+ pages: page_0 and page_1
            assert!(fifo.pages.len() >= 2);
        }

        let p1 = path.join(format!("{}1{}", PAGE_PREFIX, PAGE_EXTENSION));
        let p2 = path.join(format!("{}2{}", PAGE_PREFIX, PAGE_EXTENSION));
        std::fs::rename(&p1, &p2).unwrap();

        // Now we have page_0 and page_2. Page 1 is missing.
        // load() should now return an error because it checks for continuous sequence.
        let res = MmapFifo::<u32>::load(&path, page_size);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_load_starts_at_nonzero_id() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        // Create page_5.mmap manually with one item
        let id = 5;
        let page_path = path.join(format!("{}{}{}", PAGE_PREFIX, id, PAGE_EXTENSION));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&page_path)
            .unwrap();
        file.set_len(page_size as u64).unwrap();
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file).unwrap() };

        // Push 42 manually
        let item = 42u32;
        let bytes = postcard::to_stdvec(&item).unwrap();
        let len = bytes.len() as u32;
        let total_size = 4 + bytes.len();
        mmap[0..4].copy_from_slice(&len.to_le_bytes());
        mmap[4..total_size].copy_from_slice(&bytes);
        mmap.flush().unwrap();
        drop(mmap);

        // Load the queue. It should find only page_5.mmap.
        let mut loaded = MmapFifo::<u32>::load(&path, page_size).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.pages.len(), 1);
        assert_eq!(loaded.pages[0].id, 5);

        assert_eq!(loaded.pop().unwrap(), Some(42));
        assert_eq!(loaded.len(), 0);

        // Push another item, it should go to page 6
        // It stays in the SAME page because there's plenty of space
        loaded.push(&43).unwrap();
        assert_eq!(loaded.pages.len(), 1);
        assert_eq!(loaded.pages[0].id, 5);
        assert_eq!(loaded.pop().unwrap(), Some(43));

        // Force a new page
        for i in 0..200 {
            loaded.push(&i).unwrap();
        }
        assert!(loaded.pages.len() >= 2);
        assert_eq!(loaded.pages.back().unwrap().id, 6);
    }

    #[test]
    fn test_push_after_reload_with_rotation() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        {
            let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();
            // (4 + 2 + 506 = 512). 2 items fill 1024 exactly.
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![1u8; 506]).unwrap(); // Page 1

            assert_eq!(fifo.pages.len(), 2);

            // Pop some items from page 0
            assert_eq!(fifo.pop().unwrap().map(|v| v.len()), Some(506));
        }

        let mut loaded = MmapFifo::<Vec<u8>>::load(&path, page_size).unwrap();
        assert_eq!(loaded.len(), 2);

        // Push more items
        loaded.push(&vec![2u8; 506]).unwrap(); // Into page 1
        loaded.push(&vec![3u8; 506]).unwrap(); // New page 2

        assert_eq!(loaded.len(), 4);

        // Verify all items in order
        assert_eq!(loaded.pop().unwrap().map(|v| v.len()), Some(506)); // index 1 (page 0)
        assert_eq!(loaded.pop().unwrap().map(|v| v[0]), Some(1)); // index 2 (page 1)
        assert_eq!(loaded.pop().unwrap().map(|v| v[0]), Some(2)); // index 3 (page 1)
        assert_eq!(loaded.pop().unwrap().map(|v| v[0]), Some(3)); // index 4 (page 2)
        assert_eq!(loaded.pop().unwrap(), None);
    }

    #[test]
    fn test_clear_after_partial_pop_multi_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let page_size = 1024;

        {
            let mut fifo = MmapFifo::<Vec<u8>>::new(&path, page_size).unwrap();
            // Fill 2+ pages. Each item is header (4) + overhead (2) + data (506) = 512 bytes.
            fifo.push(&vec![0u8; 506]).unwrap();
            fifo.push(&vec![0u8; 506]).unwrap(); // Page 0 is full.
            fifo.push(&vec![1u8; 506]).unwrap();
            fifo.push(&vec![1u8; 506]).unwrap(); // Page 1 is full.
            fifo.push(&vec![2u8; 100]).unwrap(); // Into Page 2.
            assert_eq!(fifo.pages.len(), 3);

            // Pop some items to advance read_pos
            fifo.pop().unwrap();
            fifo.pop().unwrap();

            assert_eq!(fifo.len(), 3);

            fifo.clear().unwrap();
            assert_eq!(fifo.len(), 0);
            assert!(fifo.is_empty());

            // Assert disk state: only page_0.mmap should exist
            let entries: Vec<_> = std::fs::read_dir(&path)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .collect();
            assert_eq!(entries.len(), 1);
            assert!(entries.contains(&format!("{}0{}", PAGE_PREFIX, PAGE_EXTENSION)));
        }

        // Reload and verify
        let loaded = MmapFifo::<Vec<u8>>::load(&path, page_size).unwrap();
        assert_eq!(loaded.len(), 0);
        assert!(loaded.is_empty());

        // Assert disk state again
        let entries: Vec<_> = std::fs::read_dir(&path)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(entries.len(), 1);
    }
}

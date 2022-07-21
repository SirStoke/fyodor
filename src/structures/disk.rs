use integer_encoding::*;
use std::cmp::Ordering;
use std::mem;
use std::mem::size_of;
use std::ops::Index;
use thiserror::Error;

/// Represents an entry (key + value) in the LSM-tree
///
/// Can be read and created from the various helper methods. Expects an already-allocated page
/// to be written into.
///
/// The memory layout is pretty simple:
/// [ key_size, value_size, key, value ]
/// where key_size and value_size are varints
#[repr(C)]
pub struct Entry {
    data: [u8],
}

impl Entry {
    /// Returns:
    ///   - The number of bytes used by the key
    ///   - The number of bytes used by the key size
    /// respectively, given a slice which contains an Entry
    fn key_len_from_slice(data: &[u8]) -> (u32, usize) {
        u32::decode_var(data).unwrap()
    }

    /// Returns:
    ///   - The number of bytes used by the key
    ///   - The number of bytes used by the key size
    /// respectively
    fn key_len(&self) -> (u32, usize) {
        Entry::key_len_from_slice(&self.data)
    }

    /// Returns a slice containing the key
    fn key(&self) -> &[u8] {
        let (key_size, key_varint_size) = self.key_len();
        let (_, value_varint_size) = self.value_len();

        let index = key_varint_size + value_varint_size;

        &self.data[index..index + (key_size as usize)]
    }

    /// Returns:
    ///   - The number of bytes used by the value
    ///   - The number of bytes used by the value size
    /// respectively, given a slice which contains an Entry
    fn value_len_from_slice(data: &[u8]) -> (u32, usize) {
        let (_, key_varint_size) = Entry::key_len_from_slice(data);

        u32::decode_var(&data[key_varint_size..]).unwrap()
    }

    /// Returns:
    ///   - The number of bytes used by the value
    ///   - The number of bytes used by the value size
    /// respectively
    fn value_len(&self) -> (u32, usize) {
        Entry::value_len_from_slice(&self.data)
    }

    fn value(&self) -> &[u8] {
        let (key_size, key_varint_size) = self.key_len();
        let (value_size, value_varint_size) = self.value_len();

        let value_index = key_varint_size + value_varint_size + key_size as usize;

        &self.data[value_index..value_index + value_size as usize]
    }

    /// Returns the total number of bytes occupied by this entry
    fn len(&self) -> u32 {
        Entry::len_from_slice(&self.data)
    }

    fn len_from_slice(data: &[u8]) -> u32 {
        let (key_size, key_varint_size) = Entry::key_len_from_slice(data);
        let (value_size, value_varint_size) = Entry::value_len_from_slice(data);

        key_varint_size as u32 + value_varint_size as u32 + key_size + value_size
    }

    /// Creates an Entry, writing it into the memory block pointed by `page_entry`.
    /// Expects `page_entry` to have enough space
    pub fn create(block_entry: &mut [u8], key: &[u8], value: &[u8]) -> *const Entry {
        unsafe {
            let key_len = key.len();
            let key_size = key_len.encode_var(block_entry);
            let value_size = value.len().encode_var(block_entry[key_size..].as_mut());

            block_entry[key_size + value_size..key_size + value_size + key_len]
                .copy_from_slice(key);

            let value_index = key_size + value_size + key_len;
            block_entry[value_index..value_index + value.len()].copy_from_slice(value);

            mem::transmute::<&mut [u8], *const Entry>(block_entry)
        }
    }
}

#[derive(Error, Debug)]
pub enum BlockError {
    #[error("Trying to insert an Entry in a full Block")]
    FullBlock,
}

/// Frequency after which to save an index snapshot to help binary searching
const SNAPSHOT_FREQUENCY: u32 = 10;

/// An [Entry] container
///
/// A Block contains an u32 representing the size of the array, a u32 representing
/// the number of bytes currently occupied by entries (i.e. the offset the next entry will be written into),
/// and a chunk of memory containing:
///
/// - Entries, saved from the start of the chunk downwards
/// - Index snapshots, saved from the end of the chunk upwards
///
/// Index snapshots are entry offsets, saved every [SNAPSHOT_FREQUENCY], that are used by the binary
/// search algorithm
///
/// You can think of this as the equivalent of an SST Block in the RocksDB realm.
#[repr(C)]
pub struct Block {
    size: u32,
    offset: u32,
    data: [u8],
}

impl Block {
    /// Creates a new Block from a slice, ideally pointing to an mmap-ed region of memory
    pub fn new(block: *mut [u8]) -> *mut Block {
        unsafe {
            let new_block = mem::transmute::<*mut [u8], *mut Block>(block);

            (*new_block).size = 0;
            (*new_block).offset = 0;

            new_block
        }
    }

    /// Inserts a new entry into this block. Expects to be called in the right order, i.e.
    /// an earlier call must insert a key <= then a later call
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<*const Entry, BlockError> {
        let key_len = key.len();
        let value_len = value.len();

        let key_varint_size = key.len().required_space();
        let value_varint_size = key.len().required_space();

        let offset_index = self.offset as usize;
        let remaining_space = self.data.len() - offset_index;
        let entry_size = key_varint_size + value_varint_size + key_len + value_len;

        if entry_size > remaining_space {
            Err(BlockError::FullBlock)?
        }

        self.size += 1;

        if self.size % SNAPSHOT_FREQUENCY == 0 {
            self.save_offset_snapshot();
        }

        self.offset += entry_size as u32;

        Ok(Entry::create(
            self.data[offset_index..offset_index + entry_size].as_mut(),
            key,
            value,
        ))
    }

    /// Saves the current offset in the offset snapshot array
    fn save_offset_snapshot(&mut self) {
        let snapshot_index =
            self.data.len() - (self.size as usize / SNAPSHOT_FREQUENCY as usize) * size_of::<u32>();

        self.data[snapshot_index..snapshot_index + size_of::<u32>()]
            .copy_from_slice(&self.offset.to_le_bytes());
    }

    /// Retrieves the offset at the provided index from the offset snapshot array
    fn read_offset_snapshot(&self, index: usize) -> u32 {
        let snapshot_index = self.data.len() - (index + 1) * size_of::<u32>();

        u32::from_le_bytes(
            self.data[snapshot_index..snapshot_index + size_of::<u32>()]
                .try_into()
                .unwrap(),
        )
    }

    /// Reads an entry at the provided offset
    ///
    /// Unsafe because the caller must make sure that the offset is pointing at the beginning of
    /// a valid entry
    unsafe fn get_at_offset(&self, offset: u32) -> *const Entry {
        mem::transmute::<&[u8], *const Entry>(&self.data[offset as usize..])
    }

    /// Binary searches the entries in the block, using the offset snapshots as aid, comparing
    /// entries using the cmp function. It expects the searched value to actually be in the range of
    /// this block
    ///
    /// Returns the closest snapshot offset which represents a smaller (or equal) entry
    fn binary_search<T>(&self, cmp: T) -> u32
    where
        T: Fn(&[u8]) -> Ordering,
    {
        use Ordering::*;

        let mut left = 0_usize;
        let mut right = self.size as usize / SNAPSHOT_FREQUENCY as usize;

        while left < right {
            let size = right - left;
            let mid = left + size / 2;

            let offset = self.read_offset_snapshot(mid);

            // This is safe because the offsets come from the snapshots
            let entry = unsafe { self.get_at_offset(offset) };
            let order = unsafe { cmp((*entry).key()) };

            if order == Greater {
                right = mid;
            } else if order == Less {
                left = mid + 1;
            } else {
                return offset;
            }
        }

        self.read_offset_snapshot(left - 1)
    }
}

impl Index<u32> for Block {
    type Output = Entry;

    fn index(&self, index: u32) -> &Self::Output {
        match self.into_iter().nth(index as usize) {
            Some(entry) => entry,
            _ => panic!("Tried to read out of bounds index {}", index),
        }
    }
}

/// Defines the ordering between the keys
pub trait EntryOrd<Rhs = Self>
where
    Rhs: ?Sized,
{
    fn cmp(&self, other: &Rhs) -> Ordering;

    fn lt(&self, other: &Rhs) -> bool {
        self.cmp(other) == Ordering::Less
    }
}

pub struct BlockIterator<'a> {
    idx: u32,
    offset: u32,
    block: &'a Block,
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = &'a Entry;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.idx >= self.block.size {
                None
            } else {
                let data = &self.block.data;

                let entry =
                    mem::transmute::<*const [u8], *const Entry>(&data[self.offset as usize..])
                        .as_ref()
                        .unwrap();

                self.offset += entry.len();
                self.idx += 1;

                Some(entry)
            }
        }
    }
}

impl<'a> IntoIterator for &'a Block {
    type Item = &'a Entry;
    type IntoIter = BlockIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BlockIterator {
            idx: 0,
            offset: 0,
            block: self,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::structures::disk::*;
    use core::array::TryFromSliceError;
    use core::cmp::Ordering;
    use std::mem::size_of;

    #[test]
    fn create_then_read_is_consistent() {
        unsafe {
            let mut block = [0_u8; 11];

            let key: [u8; 5] = [0, 1, 2, 3, 4];
            let value: [u8; 4] = [5, 6, 7, 8];

            let entry = Entry::create(block.as_mut(), &key, &value);

            assert_eq!(entry.as_ref().unwrap().key_len(), (5, 1));
            assert_eq!(entry.as_ref().unwrap().value_len(), (4, 1));
            assert_eq!(entry.as_ref().unwrap().key(), key);
            assert_eq!(entry.as_ref().unwrap().value(), value);
        }
    }

    #[test]
    fn iterator_works() {
        // 55 for the entries + 8 for the idx + offset
        let mut block_slice = [0_u8; 55 + 8];
        let block = unsafe { &mut *Block::new(&mut block_slice as *mut [u8]) };

        let key_suffix = [0, 1, 2, 3];
        let value_suffix = [5, 6, 7];

        for n in 0..5 {
            let mut key = vec![n];

            key.extend_from_slice(&key_suffix);

            let mut value = vec![n];
            value.extend_from_slice(&value_suffix);

            block.insert(&key, &value).unwrap();
        }

        for (expected_prefix, entry) in block.into_iter().enumerate() {
            let mut expected_key = vec![expected_prefix as u8];
            expected_key.extend_from_slice(&key_suffix);

            let mut expected_value = vec![expected_prefix as u8];
            expected_value.extend_from_slice(&value_suffix);

            assert_eq!(entry.key(), expected_key.as_slice());
            assert_eq!(entry.value(), expected_value.as_slice());
        }
    }

    #[test]
    fn offset_snapshots_created_ok() {
        const SNAPSHOT_NUM: usize = 6;
        const ENTRIES_NUM: usize = SNAPSHOT_FREQUENCY as usize * SNAPSHOT_NUM;
        const ENTRIES_SIZE: usize = 11 * ENTRIES_NUM;
        const SNAPSHOTS_SIZE: usize = SNAPSHOT_NUM * size_of::<u32>();

        let mut block_slice = [0_u8; ENTRIES_SIZE + SNAPSHOTS_SIZE];

        let block = unsafe { &mut *Block::new(&mut block_slice as *mut [u8]) };

        let key_suffix = [0, 1, 2, 3];
        let value_suffix = [5, 6, 7];

        for n in 0..ENTRIES_NUM as u8 {
            let mut key = vec![n];
            key.extend_from_slice(&key_suffix);

            let mut value = vec![n];
            value.extend_from_slice(&value_suffix);

            block.insert(&key, &value).unwrap();
        }

        for n in 1..SNAPSHOT_NUM + 1 {
            let offset = block.read_offset_snapshot(n - 1);

            assert_eq!(
                offset as usize,
                (n * (SNAPSHOT_FREQUENCY as usize) - 1) * 11,
                "asserting snapshot {}",
                n
            );
        }
    }

    #[test]
    fn binary_search_ok() {
        const SNAPSHOT_NUM: usize = 6;
        const ENTRY_SIZE: usize = 11;
        const ENTRIES_NUM: usize = SNAPSHOT_FREQUENCY as usize * SNAPSHOT_NUM;
        const ENTRIES_SIZE: usize = ENTRY_SIZE * ENTRIES_NUM;
        const SNAPSHOTS_SIZE: usize = SNAPSHOT_NUM * size_of::<u32>();

        let mut block_slice = [0_u8; ENTRIES_SIZE + SNAPSHOTS_SIZE];

        let block = unsafe { &mut *Block::new(&mut block_slice as *mut [u8]) };

        let key_prefix = [0, 1, 2, 3];
        let value_suffix = [5, 6, 7];

        for n in 0..ENTRIES_NUM as u8 {
            let mut key = Vec::from(key_prefix);
            key.push(n);

            let mut value = vec![n];
            value.extend_from_slice(&value_suffix);

            block.insert(&key, &value).unwrap();
        }

        let needle_entry_num = 39;

        let mut needle = Vec::from(key_prefix);
        needle.push(needle_entry_num);

        // The needle must be 8 bytes long to be converted to an u64 below
        needle.extend_from_slice(&[0_u8; 3]);

        let res: Result<[u8; 8], TryFromSliceError> = needle.as_slice().try_into();
        let needle_int = u64::from_be_bytes(res.unwrap());

        let offset = block.binary_search(|key: &[u8]| -> Ordering {
            let mut key_int_bytes = Vec::from(key);

            key_int_bytes.extend_from_slice(&vec![0; 8 - key_int_bytes.len()]);

            let key_int = u64::from_be_bytes(key_int_bytes.try_into().unwrap());

            key_int.cmp(&needle_int)
        });

        assert_eq!(offset, needle_entry_num as u32 * ENTRY_SIZE as u32);
    }
}

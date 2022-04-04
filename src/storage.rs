use integer_encoding::*;
use std::{mem, slice};

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
    data: [u8]
}

impl Entry {
    /// Returns:
    ///   - The number of bytes used by the key
    ///   - The number of bytes used by the key size
    /// respectively
    fn key_len(&self) -> (u32, usize) {
        let data_slice = &self.data;

        u32::decode_var(data_slice).unwrap()
    }

    /// Returns a slice containing the key
    fn key(&self) -> &[u8] {
        let (key_size, key_varint_size) = self.key_len();
        let (_, value_varint_size) = self.value_len();

        let index = key_varint_size + value_varint_size;

        &self.data[index..index + (key_size as usize)]
    }

    fn value_len(&self) -> (u32, usize) {
        let (_, key_varint_size) = self.key_len();

        u32::decode_var(&self.data[key_varint_size..]).unwrap()
    }

    fn value(&self) -> &[u8] {
        let (key_size, key_varint_size) = self.key_len();
        let (value_size, value_varint_size) = self.value_len();
        
        let value_index = key_varint_size + value_varint_size + key_size as usize;

        &self.data[value_index..value_index + value_size as usize]
    }

    /// Returns the total number of bytes occupied by this entry
    fn len(&self) -> (u32) {
        let (key_size, key_varint_size) = self.key_len();
        let (value_size, value_varint_size) = self.value_len();

        return key_size + key_varint_size as u32 + value_size + value_varint_size as u32;
    }

    /// Creates an Entry, writing it into the memory block pointed by `page_entry`.
    /// Expects `page_entry` to have enough space
    pub fn create(size: usize, page_entry: *mut u8, key: &[u8], value: &[u8]) -> *const Entry {
        unsafe {
            let page_entry_slice = slice::from_raw_parts_mut(page_entry, size);
            let key_len = key.len();
            let key_size = key_len.encode_var(&mut *page_entry_slice);
            let value_size = value.len().encode_var((*page_entry_slice)[key_size..].as_mut());

            (*page_entry_slice)[key_size + value_size..key_size + value_size + key_len].copy_from_slice(key);

            let value_index = key_size + value_size + key_len;
            (*page_entry_slice)[value_index..value_index + value.len()].copy_from_slice(value);

            mem::transmute::<*mut [u8], *const Entry>(page_entry_slice)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Entry;

    #[test]
    fn create_writes_correctly() {
        unsafe {
            let mut page = [0 as u8; 11];

            let key: [u8; 5] = [0, 1, 2, 3, 4];
            let value: [u8; 4] = [5, 6, 7, 8];

            let entry = Entry::create(11, page.as_mut_ptr(), &key, &value);

            assert_eq!(entry.as_ref().unwrap().key_len(), (5, 1));
            assert_eq!(entry.as_ref().unwrap().value_len(), (4, 1));
            assert_eq!(entry.as_ref().unwrap().key(), key);
            assert_eq!(entry.as_ref().unwrap().value(), value);
        }
    }
}
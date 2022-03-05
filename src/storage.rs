use integer_encoding::*;
use std::{mem, slice};

#[repr(C)]
pub struct Entry {
    data: [u8]
}

// Represents an entry (key + value) in the LSM-tree
//
// Can be read and created from the various helper methods. Expects an already-allocated page
// to be written into.
//
// The memory layout is pretty simple:
// [ key_size, key, value_size, value ]
// where key_size and value_size are varints
impl Entry {
    /// Returns:
    ///   - The number of bytes used by the key
    ///   - The number of bytes used by the key size
    /// respectively
    fn key_len(&self) -> (u32, usize) {
        let data_slice = &self.data;

        u32::decode_var(data_slice).unwrap()
    }

    /// Returns a slice pointing to
    fn key(&self) -> &[u8] {
        let (key_size, varint_size) = self.key_len();

        &self.data[varint_size..(key_size as usize) + 1]
    }

    fn value_len(&self) -> (u32, usize) {
        let (key_size, key_varint_size) = self.key_len();

        u32::decode_var(&self.data[key_size as usize + key_varint_size..]).unwrap()
    }

    fn value(&self) -> &[u8] {
        let (key_size, key_varint_size) = self.key_len();
        let (_, value_varint_size) = self.value_len();

        &self.data[key_varint_size + key_size as usize + value_varint_size..]
    }

    /// Creates an Entry, writing it into the memory block pointed by `page_entry`.
    /// Expects `page_entry` to have enough space
    pub fn create(size: usize, page_entry: *mut u8, key: &[u8], value: &[u8]) -> *const Entry {
        unsafe {
            let page_entry_slice = slice::from_raw_parts_mut(page_entry, size);
            let key_len = key.len();
            let key_size = key_len.encode_var(&mut *page_entry_slice);
            (*page_entry_slice)[key_size..key_size + key_len].copy_from_slice(key);

            let value_size = value.len().encode_var((*page_entry_slice)[key_size + key_len..].as_mut());
            let value_index = key_size + key_len + value_size;
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
            let mut page = [0 as u8; 10];

            let key: [u8; 4] = [0, 1, 2, 3];
            let value: [u8; 4] = [4, 5, 6, 7];

            let entry = Entry::create(10, page.as_mut_ptr(), &key, &value);

            assert_eq!(entry.as_ref().unwrap().key(), key);
            assert_eq!(entry.as_ref().unwrap().value(), value);
        }
    }
}
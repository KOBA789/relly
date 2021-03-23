use std::mem::size_of;
use std::ops::{Index, IndexMut, Range};

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    num_slots: u16,
    free_space_offset: u16,
    _pad: u32,
}

#[derive(Debug, FromBytes, AsBytes, Clone, Copy)]
#[repr(C)]
pub struct Pointer {
    offset: u16,
    len: u16,
}

impl Pointer {
    fn range(&self) -> Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        start..end
    }
}

pub type Pointers<B> = LayoutVerified<B, [Pointer]>;

pub struct Slotted<B> {
    header: LayoutVerified<B, Header>,
    body: B,
}

impl<B: ByteSlice> Slotted<B> {
    pub fn new(bytes: B) -> Self {
        let (header, body) =
            LayoutVerified::new_from_prefix(bytes).expect("slotted header must be aligned");
        Self { header, body }
    }

    pub fn capacity(&self) -> usize {
        self.body.len()
    }

    pub fn num_slots(&self) -> usize {
        self.header.num_slots as usize
    }

    pub fn free_space(&self) -> usize {
        self.header.free_space_offset as usize - self.pointers_size()
    }

    fn pointers_size(&self) -> usize {
        size_of::<Pointer>() * self.num_slots()
    }

    fn pointers(&self) -> Pointers<&[u8]> {
        Pointers::new_slice(&self.body[..self.pointers_size()]).unwrap()
    }

    fn data(&self, pointer: Pointer) -> &[u8] {
        &self.body[pointer.range()]
    }
}

impl<B: ByteSliceMut> Slotted<B> {
    pub fn initialize(&mut self) {
        self.header.num_slots = 0;
        self.header.free_space_offset = self.body.len() as u16;
    }

    fn pointers_mut(&mut self) -> Pointers<&mut [u8]> {
        let pointers_size = self.pointers_size();
        Pointers::new_slice(&mut self.body[..pointers_size]).unwrap()
    }

    fn data_mut(&mut self, pointer: Pointer) -> &mut [u8] {
        &mut self.body[pointer.range()]
    }

    pub fn insert(&mut self, index: usize, len: usize) -> Option<()> {
        if self.free_space() < size_of::<Pointer>() + len {
            return None;
        }
        let num_slots_orig = self.num_slots();
        self.header.free_space_offset -= len as u16;
        self.header.num_slots += 1;
        let free_space_offset = self.header.free_space_offset;
        let mut pointers_mut = self.pointers_mut();
        pointers_mut.copy_within(index..num_slots_orig, index + 1);
        let pointer = &mut pointers_mut[index];
        pointer.offset = free_space_offset;
        pointer.len = len as u16;
        Some(())
    }

    pub fn remove(&mut self, index: usize) {
        self.resize(index, 0);
        self.pointers_mut().copy_within(index + 1.., index);
        self.header.num_slots -= 1;
    }

    pub fn resize(&mut self, index: usize, len_new: usize) -> Option<()> {
        let pointers = self.pointers();
        let len_orig = pointers[index].len;
        let len_incr = len_new as isize - len_orig as isize;
        if len_incr == 0 {
            return Some(());
        }
        if len_incr > self.free_space() as isize {
            return None;
        }
        let free_space_offset = self.header.free_space_offset as usize;
        let offset_orig = pointers[index].offset;
        let shift_range = free_space_offset..offset_orig as usize;
        let free_space_offset_new = (free_space_offset as isize - len_incr) as usize;
        self.header.free_space_offset = free_space_offset_new as u16;
        self.body
            .as_bytes_mut()
            .copy_within(shift_range, free_space_offset_new);
        let mut pointers_mut = self.pointers_mut();
        for pointer in pointers_mut.iter_mut() {
            if pointer.offset <= offset_orig {
                pointer.offset = (pointer.offset as isize - len_incr) as u16;
            }
        }
        let pointer = &mut pointers_mut[index];
        pointer.len = len_new as u16;
        if len_new == 0 {
            pointer.offset = free_space_offset_new as u16;
        }
        Some(())
    }
}

impl<B: ByteSlice> Index<usize> for Slotted<B> {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        self.data(self.pointers()[index])
    }
}

impl<B: ByteSliceMut> IndexMut<usize> for Slotted<B> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut(self.pointers()[index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut page_data = vec![0u8; 128];
        let mut slotted = Slotted::new(page_data.as_mut_slice());
        let insert = |slotted: &mut Slotted<&mut [u8]>, index: usize, buf: &[u8]| {
            slotted.insert(index, buf.len()).unwrap();
            slotted[index].copy_from_slice(buf);
        };
        let push = |slotted: &mut Slotted<&mut [u8]>, buf: &[u8]| {
            let index = slotted.num_slots() as usize;
            insert(slotted, index, buf);
        };
        slotted.initialize();
        push(&mut slotted, b"hello");
        push(&mut slotted, b"world");
        assert_eq!(&slotted[0], b"hello");
        assert_eq!(&slotted[1], b"world");
        insert(&mut slotted, 1, b", ");
        push(&mut slotted, b"!");
        assert_eq!(&slotted[0], b"hello");
        assert_eq!(&slotted[1], b", ");
        assert_eq!(&slotted[2], b"world");
        assert_eq!(&slotted[3], b"!");
    }
}

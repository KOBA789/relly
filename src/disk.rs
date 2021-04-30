use core::convert::TryInto;
use alloc::vec::Vec;

use zerocopy::{AsBytes, FromBytes};

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, FromBytes, AsBytes)]
#[repr(C)]
pub struct PageId(pub u64);
impl PageId {
    pub const INVALID_PAGE_ID: PageId = PageId(u64::MAX);

    pub fn valid(self) -> Option<PageId> {
        if self == Self::INVALID_PAGE_ID {
            None
        } else {
            Some(self)
        }
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }
}

impl Default for PageId {
    fn default() -> Self {
        Self::INVALID_PAGE_ID
    }
}

impl From<Option<PageId>> for PageId {
    fn from(page_id: Option<PageId>) -> Self {
        page_id.unwrap_or_default()
    }
}

impl From<&[u8]> for PageId {
    fn from(bytes: &[u8]) -> Self {
        let arr = bytes.try_into().unwrap();
        PageId(u64::from_ne_bytes(arr))
    }
}

pub struct DiskManager {
    heap_buffer: Vec<u8>,
    next_page_id: u64,
}

impl DiskManager {
    pub fn new(heap_buffer: Vec<u8>, next_page_id: u64) -> Self {
        Self {
            heap_buffer,
            next_page_id,
        }
    }

    pub fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) {
        let offset = PAGE_SIZE * page_id.to_u64() as usize;
        data.copy_from_slice(&self.heap_buffer[offset..offset + PAGE_SIZE]);
    }

    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) {
        let offset = PAGE_SIZE * page_id.to_u64() as usize;
        self.heap_buffer[offset..offset + PAGE_SIZE].copy_from_slice(data);
    }

    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut disk = DiskManager::new(vec![0u8; PAGE_SIZE * 1024], 0);
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);
        let hello_page_id = disk.allocate_page();
        disk.write_page_data(hello_page_id, &hello);
        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);
        let world_page_id = disk.allocate_page();
        disk.write_page_data(world_page_id, &world);
        let mut buf = vec![0; PAGE_SIZE];
        disk.read_page_data(hello_page_id, &mut buf);
        assert_eq!(hello, buf);
        disk.read_page_data(world_page_id, &mut buf);
        assert_eq!(world, buf);
    }
}

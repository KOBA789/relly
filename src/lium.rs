use core::alloc::{GlobalAlloc, Layout};

const MALLOC_MAX_SIZE: usize = 1024 * 1024;
static mut MALLOC_SIZE: usize = 0;
static mut MALLOC_ARRAY: [u8; MALLOC_MAX_SIZE] =
    [0u8; MALLOC_MAX_SIZE];

struct LiumAllocator;

unsafe impl GlobalAlloc for LiumAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MALLOC_SIZE + layout.size() >
            MALLOC_ARRAY.len() {
            panic!("failed to allocate memory");
        }
        let ptr = MALLOC_ARRAY[MALLOC_SIZE..].as_mut_ptr();
        MALLOC_SIZE += layout.size();
        ptr
    }
    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {}
}

#[global_allocator]
static A: LiumAllocator = LiumAllocator;

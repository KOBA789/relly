#![feature(default_alloc_error_handler)]

#![no_std]
#![no_main]
#[macro_use]
extern crate alloc;

use core::panic::PanicInfo;

use relly::{disk::{DiskManager, PageId}, query::PlanNode};
use relly::buffer::{BufferPool, BufferPoolManager};
use relly::table::Table;
use relly::query::{SeqScan, TupleSearchMode};
use relly::tuple::Pretty;

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

#[link(name = "liumos", kind = "static")]
extern "C" {
    fn sys_write(fp: i32, str: *const u8, len: u64);
    fn sys_exit(code: i32);
}

fn relly_main() {
    let disk = DiskManager::new(
        vec![0u8; 4096 * 64],
        0
    );
    let pool = BufferPool::new(10);
    let mut bufmgr =
        BufferPoolManager::new(disk, pool);
    let mut table = Table {
        meta_page_id: PageId::INVALID_PAGE_ID,
        num_key_elems: 1,
        unique_indices: vec![],
    };
    table.create(&mut bufmgr).unwrap();
    table.insert(&mut bufmgr,
        &[b"1", b"hello", b"world"]).unwrap();
    table.insert(&mut bufmgr,
        &[b"2", b"foo", b"bar"]).unwrap();

    let plan = SeqScan {
        table_meta_page_id: table.meta_page_id,
        search_mode: TupleSearchMode::Key(&[b"1"]),
        while_cond: &|key|
            key[0].as_slice() == b"1",
    };
    let mut exec =
        plan.start(&mut bufmgr).unwrap();
    while let Some(record) =
        exec.next(&mut bufmgr).unwrap() {
        let s = format!("{:?}\n", Pretty(&record));
        print(&s);
    }
}

fn print(s: &str) {
    unsafe {
        sys_write(1, s.as_ptr(), s.len() as u64);
    }
}

fn exit(code: i32) {
    unsafe {
        sys_exit(code);
    }
}

#[no_mangle]
pub extern "C" fn _start() -> ! {
    relly_main();
    exit(0);
    loop {}
}

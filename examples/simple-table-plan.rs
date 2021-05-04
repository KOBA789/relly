use std::rc::Rc;

use anyhow::Result;

use relly::query::{Filter, SeqScan, TupleSearchMode, PlanNode};
use relly::buffer::{BufferPool, BufferPoolManager};
use relly::disk::{DiskManager, PageId};
use relly::tuple;

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let plan = Filter {
        cond: Rc::new(|record| record[1].as_slice() < b"Dave"),
        inner_plan: Box::new(SeqScan {
            table_meta_page_id: PageId(0),
            search_mode: TupleSearchMode::Key(vec![b"w".to_vec()]),
            while_cond: Rc::new(|pkey| pkey[0].as_slice() < b"z"),
        }),
    };
    let mut exec = plan.start(&mut bufmgr)?;

    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}

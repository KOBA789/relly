use std::rc::Rc;

use anyhow::Result;

use crate::btree::{self, BTree, SearchMode};
use crate::buffer::BufferPoolManager;
use crate::disk::PageId;
use crate::tuple;

pub type Tuple = Vec<Vec<u8>>;
pub type TupleSlice<'a> = &'a [Vec<u8>];

pub enum TupleSearchMode {
    Start,
    Key(Tuple),
}

impl TupleSearchMode {
    fn encode(&self) -> SearchMode {
        match self {
            TupleSearchMode::Start => SearchMode::Start,
            TupleSearchMode::Key(tuple) => {
                let mut key = vec![];
                tuple::encode(tuple.iter(), &mut key);
                SearchMode::Key(key)
            }
        }
    }
}

pub trait Executor {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>>;
}

pub type BoxExecutor<'a> = Box<dyn Executor + 'a>;

pub trait PlanNode {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor>;
}

pub struct SeqScan {
    pub table_meta_page_id: PageId,
    pub search_mode: TupleSearchMode,
    pub while_cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl PlanNode for SeqScan {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor> {
        let btree = BTree::new(self.table_meta_page_id);
        let table_iter = btree.search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecSeqScan {
            table_iter,
            while_cond: self.while_cond.clone(),
        }))
    }
}

pub struct ExecSeqScan {
    table_iter: btree::Iter,
    while_cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl Executor for ExecSeqScan {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>> {
        let (pkey_bytes, tuple_bytes) = match self.table_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut pkey = vec![];
        tuple::decode(&pkey_bytes, &mut pkey);
        if !(self.while_cond)(&pkey) {
            return Ok(None);
        }
        let mut tuple = pkey;
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct Filter {
    pub inner_plan: Box<dyn PlanNode>,
    pub cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl PlanNode for Filter {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor> {
        let inner_iter = self.inner_plan.start(bufmgr)?;
        Ok(Box::new(ExecFilter {
            inner_iter,
            cond: self.cond.clone(),
        }))
    }
}

pub struct ExecFilter<'a> {
    inner_iter: BoxExecutor<'a>,
    cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl<'a> Executor for ExecFilter<'a> {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>> {
        loop {
            match self.inner_iter.next(bufmgr)? {
                Some(tuple) => {
                    if (self.cond)(&tuple) {
                        return Ok(Some(tuple));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

pub struct IndexScan {
    pub table_meta_page_id: PageId,
    pub index_meta_page_id: PageId,
    pub search_mode: TupleSearchMode,
    pub while_cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl PlanNode for IndexScan {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor> {
        let table_btree = BTree::new(self.table_meta_page_id);
        let index_btree = BTree::new(self.index_meta_page_id);
        let index_iter = index_btree.search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecIndexScan {
            table_btree,
            index_iter,
            while_cond: self.while_cond.clone(),
        }))
    }
}

pub struct ExecIndexScan {
    table_btree: BTree,
    index_iter: btree::Iter,
    while_cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl Executor for ExecIndexScan {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut skey = vec![];
        tuple::decode(&skey_bytes, &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let mut table_iter = self
            .table_btree
            .search(bufmgr, SearchMode::Key(pkey_bytes))?;
        let (pkey_bytes, tuple_bytes) = table_iter.next(bufmgr)?.unwrap();
        let mut tuple = vec![];
        tuple::decode(&pkey_bytes, &mut tuple);
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct IndexOnlyScan {
    pub index_meta_page_id: PageId,
    pub search_mode: TupleSearchMode,
    pub while_cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl PlanNode for IndexOnlyScan {
    fn start(&self, bufmgr: &mut BufferPoolManager) -> Result<BoxExecutor> {
        let btree = BTree::new(self.index_meta_page_id);
        let index_iter = btree.search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecIndexOnlyScan {
            index_iter,
            while_cond: self.while_cond.clone(),
        }))
    }
}

pub struct ExecIndexOnlyScan {
    index_iter: btree::Iter,
    while_cond: Rc<dyn Fn(TupleSlice) -> bool>,
}

impl Executor for ExecIndexOnlyScan {
    fn next(&mut self, bufmgr: &mut BufferPoolManager) -> Result<Option<Tuple>> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut skey = vec![];
        tuple::decode(&skey_bytes, &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let mut tuple = skey;
        tuple::decode(&pkey_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

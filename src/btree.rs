use std::cell::{Ref, RefMut};
use std::convert::identity;
use std::rc::Rc;

use bincode::Options;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{AsBytes, ByteSlice};

use crate::buffer::{self, Buffer, BufferPoolManager};
use crate::disk::PageId;

mod branch;
mod leaf;
mod meta;
mod node;

#[derive(Serialize, Deserialize)]
pub struct Pair<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Pair<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::options().serialize(self).unwrap()
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        bincode::options().deserialize(bytes).unwrap()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] buffer::Error),
}

#[derive(Debug, Clone)]
pub enum SearchMode {
    Start,
    Key(Vec<u8>),
}

impl SearchMode {
    fn child_page_id(&self, branch: &branch::Branch<impl ByteSlice>) -> PageId {
        match self {
            SearchMode::Start => branch.child_at(0),
            SearchMode::Key(key) => branch.search_child(key),
        }
    }

    fn tuple_slot_id(&self, leaf: &leaf::Leaf<impl ByteSlice>) -> Result<usize, usize> {
        match self {
            SearchMode::Start => Err(0),
            SearchMode::Key(key) => leaf.search_slot_id(key),
        }
    }
}

pub struct BTree {
    pub meta_page_id: PageId,
}

impl BTree {
    pub fn create(bufmgr: &mut BufferPoolManager) -> Result<Self, Error> {
        let meta_buffer = bufmgr.create_page()?;
        let mut meta = meta::Meta::new(meta_buffer.page.borrow_mut() as RefMut<[_]>);
        let root_buffer = bufmgr.create_page()?;
        let mut root = node::Node::new(root_buffer.page.borrow_mut() as RefMut<[_]>);
        root.initialize_as_leaf();
        let mut leaf = leaf::Leaf::new(root.body);
        leaf.initialize();
        meta.header.root_page_id = root_buffer.page_id;
        Ok(Self::new(meta_buffer.page_id))
    }

    pub fn new(meta_page_id: PageId) -> Self {
        Self { meta_page_id }
    }

    fn fetch_root_page(&self, bufmgr: &mut BufferPoolManager) -> Result<Rc<Buffer>, Error> {
        let root_page_id = {
            let meta_buffer = bufmgr.fetch_page(self.meta_page_id)?;
            let meta = meta::Meta::new(meta_buffer.page.borrow() as Ref<[_]>);
            meta.header.root_page_id
        };
        Ok(bufmgr.fetch_page(root_page_id)?)
    }

    fn search_internal(
        &self,
        bufmgr: &mut BufferPoolManager,
        node_buffer: Rc<Buffer>,
        search_mode: SearchMode,
    ) -> Result<Iter, Error> {
        let node = node::Node::new(node_buffer.page.borrow() as Ref<[_]>);
        match node::Body::new(node.header.node_type, node.body.as_bytes()) {
            node::Body::Leaf(leaf) => {
                let slot_id = search_mode.tuple_slot_id(&leaf).unwrap_or_else(identity);
                let is_right_most = leaf.num_pairs() == slot_id;
                drop(node);

                let mut iter = Iter {
                    buffer: node_buffer,
                    slot_id,
                };
                if is_right_most {
                    iter.advance(bufmgr)?;
                }
                Ok(iter)
            }
            node::Body::Branch(branch) => {
                let child_page_id = search_mode.child_page_id(&branch);
                drop(node);
                drop(node_buffer);
                let child_node_page = bufmgr.fetch_page(child_page_id)?;
                self.search_internal(bufmgr, child_node_page, search_mode)
            }
        }
    }

    pub fn search(
        &self,
        bufmgr: &mut BufferPoolManager,
        search_mode: SearchMode,
    ) -> Result<Iter, Error> {
        let root_page = self.fetch_root_page(bufmgr)?;
        self.search_internal(bufmgr, root_page, search_mode)
    }

    fn insert_internal(
        &self,
        bufmgr: &mut BufferPoolManager,
        buffer: Rc<Buffer>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, PageId)>, Error> {
        let node = node::Node::new(buffer.page.borrow_mut() as RefMut<[_]>);
        match node::Body::new(node.header.node_type, node.body) {
            node::Body::Leaf(mut leaf) => {
                let slot_id = match leaf.search_slot_id(key) {
                    Ok(_) => return Err(Error::DuplicateKey),
                    Err(slot_id) => slot_id,
                };
                if leaf.insert(slot_id, key, value).is_some() {
                    buffer.is_dirty.set(true);
                    Ok(None)
                } else {
                    let prev_leaf_page_id = leaf.prev_page_id();
                    let prev_leaf_buffer = prev_leaf_page_id
                        .map(|next_leaf_page_id| bufmgr.fetch_page(next_leaf_page_id))
                        .transpose()?;

                    let new_leaf_buffer = bufmgr.create_page()?;

                    if let Some(prev_leaf_buffer) = prev_leaf_buffer {
                        let node =
                            node::Node::new(prev_leaf_buffer.page.borrow_mut() as RefMut<[_]>);
                        let mut prev_leaf = leaf::Leaf::new(node.body);
                        prev_leaf.set_next_page_id(Some(new_leaf_buffer.page_id));
                        prev_leaf_buffer.is_dirty.set(true);
                    }
                    leaf.set_prev_page_id(Some(new_leaf_buffer.page_id));

                    let mut new_leaf_node =
                        node::Node::new(new_leaf_buffer.page.borrow_mut() as RefMut<[_]>);
                    new_leaf_node.initialize_as_leaf();
                    let mut new_leaf = leaf::Leaf::new(new_leaf_node.body);
                    new_leaf.initialize();
                    let overflow_key = leaf.split_insert(&mut new_leaf, key, value);
                    new_leaf.set_next_page_id(Some(buffer.page_id));
                    new_leaf.set_prev_page_id(prev_leaf_page_id);
                    buffer.is_dirty.set(true);
                    Ok(Some((overflow_key, new_leaf_buffer.page_id)))
                }
            }
            node::Body::Branch(mut branch) => {
                let child_idx = branch.search_child_idx(key);
                let child_page_id = branch.child_at(child_idx);
                let child_node_buffer = bufmgr.fetch_page(child_page_id)?;
                if let Some((overflow_key_from_child, overflow_child_page_id)) =
                    self.insert_internal(bufmgr, child_node_buffer, key, value)?
                {
                    if branch
                        .insert(child_idx, &overflow_key_from_child, overflow_child_page_id)
                        .is_some()
                    {
                        buffer.is_dirty.set(true);
                        Ok(None)
                    } else {
                        let new_branch_buffer = bufmgr.create_page()?;
                        let mut new_branch_node =
                            node::Node::new(new_branch_buffer.page.borrow_mut() as RefMut<[_]>);
                        new_branch_node.initialize_as_branch();
                        let mut new_branch = branch::Branch::new(new_branch_node.body);
                        let overflow_key = branch.split_insert(
                            &mut new_branch,
                            &overflow_key_from_child,
                            overflow_child_page_id,
                        );
                        buffer.is_dirty.set(true);
                        new_branch_buffer.is_dirty.set(true);
                        Ok(Some((overflow_key, new_branch_buffer.page_id)))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn insert(
        &self,
        bufmgr: &mut BufferPoolManager,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Error> {
        let meta_buffer = bufmgr.fetch_page(self.meta_page_id)?;
        let mut meta = meta::Meta::new(meta_buffer.page.borrow_mut() as RefMut<[_]>);
        let root_page_id = meta.header.root_page_id;
        let root_buffer = bufmgr.fetch_page(root_page_id)?;
        if let Some((key, child_page_id)) = self.insert_internal(bufmgr, root_buffer, key, value)? {
            let new_root_buffer = bufmgr.create_page()?;
            let mut node = node::Node::new(new_root_buffer.page.borrow_mut() as RefMut<[_]>);
            node.initialize_as_branch();
            let mut branch = branch::Branch::new(node.body);
            branch.initialize(&key, child_page_id, root_page_id);
            meta.header.root_page_id = new_root_buffer.page_id;
            meta_buffer.is_dirty.set(true);
        }
        Ok(())
    }
}

pub struct Iter {
    buffer: Rc<Buffer>,
    slot_id: usize,
}

impl Iter {
    fn get(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let leaf_node = node::Node::new(self.buffer.page.borrow() as Ref<[_]>);
        let leaf = leaf::Leaf::new(leaf_node.body);
        if self.slot_id < leaf.num_pairs() {
            let pair = leaf.pair_at(self.slot_id);
            Some((pair.key.to_vec(), pair.value.to_vec()))
        } else {
            None
        }
    }

    fn advance(&mut self, bufmgr: &mut BufferPoolManager) -> Result<(), Error> {
        self.slot_id += 1;
        let next_page_id = {
            let leaf_node = node::Node::new(self.buffer.page.borrow() as Ref<[_]>);
            let leaf = leaf::Leaf::new(leaf_node.body);
            if self.slot_id < leaf.num_pairs() {
                return Ok(());
            }
            leaf.next_page_id()
        };
        if let Some(next_page_id) = next_page_id {
            self.buffer = bufmgr.fetch_page(next_page_id)?;
            self.slot_id = 0;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn next(
        &mut self,
        bufmgr: &mut BufferPoolManager,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let value = self.get();
        self.advance(bufmgr)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use crate::{buffer::BufferPool, disk::DiskManager};

    use super::*;
    #[test]
    fn test() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let btree = BTree::create(&mut bufmgr).unwrap();
        btree
            .insert(&mut bufmgr, &6u64.to_be_bytes(), b"world")
            .unwrap();
        btree
            .insert(&mut bufmgr, &3u64.to_be_bytes(), b"hello")
            .unwrap();
        btree
            .insert(&mut bufmgr, &8u64.to_be_bytes(), b"!")
            .unwrap();
        btree
            .insert(&mut bufmgr, &4u64.to_be_bytes(), b",")
            .unwrap();

        let (_, value) = btree
            .search(&mut bufmgr, SearchMode::Key(3u64.to_be_bytes().to_vec()))
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(b"hello", &value[..]);
        let (_, value) = btree
            .search(&mut bufmgr, SearchMode::Key(8u64.to_be_bytes().to_vec()))
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(b"!", &value[..]);
    }

    #[test]
    fn test_search_iter() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let btree = BTree::create(&mut bufmgr).unwrap();

        for i in 0u64..16 {
            btree
                .insert(&mut bufmgr, &(i * 2).to_be_bytes(), &[0; 1024])
                .unwrap();
        }

        for i in 0u64..15 {
            let (key, _) = btree
                .search(
                    &mut bufmgr,
                    SearchMode::Key((i * 2 + 1).to_be_bytes().to_vec()),
                )
                .unwrap()
                .get()
                .unwrap();
            assert_eq!(key.as_slice(), &((i + 1) * 2).to_be_bytes());
        }
    }

    #[test]
    fn test_split() {
        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let btree = BTree::create(&mut bufmgr).unwrap();
        let long_data_list = vec![
            vec![0xC0u8; 1000],
            vec![0x01u8; 1000],
            vec![0xCAu8; 1000],
            vec![0xFEu8; 1000],
            vec![0xDEu8; 1000],
            vec![0xADu8; 1000],
            vec![0xBEu8; 1000],
            vec![0xAEu8; 1000],
        ];
        for data in long_data_list.iter() {
            btree.insert(&mut bufmgr, data, data).unwrap();
        }
        for data in long_data_list.iter() {
            let (k, v) = btree
                .search(&mut bufmgr, SearchMode::Key(data.clone()))
                .unwrap()
                .get()
                .unwrap();
            assert_eq!(data, &k);
            assert_eq!(data, &v);
        }
    }
}

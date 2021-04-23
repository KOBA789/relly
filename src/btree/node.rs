use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use super::branch::Branch;
use super::leaf::Leaf;

pub const NODE_TYPE_LEAF: [u8; 8] = *b"LEAF    ";
pub const NODE_TYPE_BRANCH: [u8; 8] = *b"BRANCH  ";

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    pub node_type: [u8; 8],
}

pub struct Node<B> {
    pub header: LayoutVerified<B, Header>,
    pub body: B,
}

impl<B: ByteSlice> Node<B> {
    pub fn new(bytes: B) -> Self {
        let (header, body) = LayoutVerified::new_from_prefix(bytes).expect("node must be aligned");
        Self { header, body }
    }
}

impl<B: ByteSliceMut> Node<B> {
    pub fn initialize_as_leaf(&mut self) {
        self.header.node_type = NODE_TYPE_LEAF;
    }

    pub fn initialize_as_branch(&mut self) {
        self.header.node_type = NODE_TYPE_BRANCH;
    }
}

pub enum Body<B> {
    Leaf(Leaf<B>),
    Branch(Branch<B>),
}

impl<B: ByteSlice> Body<B> {
    pub fn new(node_type: [u8; 8], bytes: B) -> Body<B> {
        match node_type {
            NODE_TYPE_LEAF => Body::Leaf(Leaf::new(bytes)),
            NODE_TYPE_BRANCH => Body::Branch(Branch::new(bytes)),
            _ => unreachable!(),
        }
    }
}

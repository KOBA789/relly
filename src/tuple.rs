use std::fmt::{self, Debug};

use crate::memcmpable;

pub fn encode(elems: impl Iterator<Item = impl AsRef<[u8]>>, bytes: &mut Vec<u8>) {
    elems.for_each(|elem| {
        let elem_bytes = elem.as_ref();
        let len = memcmpable::encoded_size(elem_bytes.len());
        bytes.reserve(len);
        memcmpable::encode(elem_bytes, bytes);
    });
}

pub fn decode(bytes: &[u8], elems: &mut Vec<Vec<u8>>) {
    let mut rest = bytes;
    while !rest.is_empty() {
        let mut elem = vec![];
        memcmpable::decode(&mut rest, &mut elem);
        elems.push(elem);
    }
}

pub struct Pretty<'a, T>(pub &'a [T]);

impl<'a, T: AsRef<[u8]>> Debug for Pretty<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_tuple("Tuple");
        for elem in self.0 {
            let bytes = elem.as_ref();
            match std::str::from_utf8(&bytes) {
                Ok(s) => {
                    d.field(&format_args!("{:?} {:02x?}", s, bytes));
                }
                Err(_) => {
                    d.field(&format_args!("{:02x?}", bytes));
                }
            }
        }
        d.finish()
    }
}

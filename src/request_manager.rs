use std::collections::VecDeque;

use crate::{
    access::{Access, AccessLike, Read},
    address::ByteAddress,
};

#[derive(Default, Debug)]
pub struct RequestManager {
    addr_to_access_map: fxhash::FxHashMap<ByteAddress, VecDeque<Read>>,
}

impl RequestManager {
    pub fn add_request(&mut self, access: Read) {
        self.addr_to_access_map
            .entry(access.get_addr())
            .or_default()
            .push_back(access);
    }

    /// Possibly returns an access if it is ready.
    pub fn register_recv(&mut self, addr: ByteAddress) -> Read {
        match self.addr_to_access_map.get_mut(&addr) {
            Some(queue) if !queue.is_empty() => {
                let mut acc = queue.pop_front().unwrap();
                acc.mark_resolved();
                acc
            }
            _ => {
                panic!("We didn't have a request registered at this address!");
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.addr_to_access_map
            .values()
            .all(|queue| queue.is_empty())
    }
}

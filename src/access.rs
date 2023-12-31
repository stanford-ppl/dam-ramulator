use std::{cell::RefCell, rc::Rc};

use derive_more::Constructor;
use enum_dispatch::enum_dispatch;

use crate::address::ByteAddress;

#[enum_dispatch]
pub trait AccessLike {
    fn mark_resolved(&mut self) {}
    fn get_addr(&self) -> ByteAddress;
    fn is_write(&self) -> bool;

    fn bundle_index(&self) -> usize;
    fn num_chunks(&self) -> u64;
}

#[enum_dispatch(AccessLike)]
#[derive(Clone, Debug)]
pub enum Access<T> {
    SimpleRead(SimpleRead),
    SimpleWrite(SimpleWrite<T>),

    ComplexRead(ComplexRead),
    ComplexWrite(ComplexWrite<T>),
}

#[enum_dispatch(AccessLike)]
#[derive(Clone, Debug)]
pub enum Read {
    SimpleRead,
    ComplexRead,
}

#[derive(Clone, Copy, Constructor, Debug)]
pub struct SimpleRead {
    base: ByteAddress,
    bundle_index: usize,
}

impl AccessLike for SimpleRead {
    fn get_addr(&self) -> ByteAddress {
        self.base
    }

    fn is_write(&self) -> bool {
        false
    }

    fn bundle_index(&self) -> usize {
        self.bundle_index
    }

    fn num_chunks(&self) -> u64 {
        1
    }
}

#[derive(Clone, Constructor, Debug)]
pub struct SimpleWrite<T> {
    base: ByteAddress,
    pub payload: T,
    bundle_index: usize,
}

impl<T> AccessLike for SimpleWrite<T> {
    fn get_addr(&self) -> ByteAddress {
        self.base
    }

    fn is_write(&self) -> bool {
        true
    }

    fn bundle_index(&self) -> usize {
        self.bundle_index
    }

    fn num_chunks(&self) -> u64 {
        1
    }
}

#[derive(Constructor, Debug)]
pub struct ComplexAccessData {
    // Base address
    base: ByteAddress,

    // Number of underlying memory chunks
    num_chunks: u64,

    // How many sub-blocks have been loaded already
    matched_blocks: u64,
    bundle_index: usize,
}

impl ComplexAccessData {
    fn mark_resolved(&mut self) {
        self.matched_blocks += 1;
    }

    fn almost_ready(&self) -> bool {
        (self.matched_blocks + 1) >= self.num_chunks
    }
}

#[derive(Clone, Constructor, Debug)]
pub struct ComplexAccessProxy {
    data: Rc<RefCell<ComplexAccessData>>,
}

impl std::ops::Deref for ComplexAccessProxy {
    type Target = Rc<RefCell<ComplexAccessData>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for ComplexAccessProxy {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[derive(Clone, Constructor, Debug)]
pub struct ComplexRead {
    proxy: ComplexAccessProxy,
    offset: u64,
}

impl AccessLike for ComplexRead {
    fn mark_resolved(&mut self) {
        RefCell::borrow_mut(&self.proxy).mark_resolved()
    }

    fn get_addr(&self) -> ByteAddress {
        self.proxy.data.borrow().base + self.offset
    }

    fn is_write(&self) -> bool {
        false
    }

    fn bundle_index(&self) -> usize {
        self.proxy.data.borrow().bundle_index
    }

    fn num_chunks(&self) -> u64 {
        self.proxy.data.borrow().num_chunks
    }
}

impl ComplexRead {
    pub fn almost_ready(&self) -> bool {
        RefCell::borrow(&self.proxy).almost_ready()
    }

    pub fn base_addr(&self) -> ByteAddress {
        self.proxy.data.borrow().base
    }
}

#[derive(Clone, Constructor, Debug)]
pub struct ComplexWrite<T> {
    proxy: ComplexAccessProxy,
    offset: u64,
    pub payload: Rc<T>,
}

impl<T> AccessLike for ComplexWrite<T> {
    fn mark_resolved(&mut self) {
        RefCell::borrow_mut(&self.proxy).mark_resolved()
    }

    fn get_addr(&self) -> ByteAddress {
        self.proxy.data.borrow().base + self.offset
    }

    fn is_write(&self) -> bool {
        true
    }
    fn bundle_index(&self) -> usize {
        self.proxy.data.borrow().bundle_index
    }

    fn num_chunks(&self) -> u64 {
        self.proxy.data.borrow().num_chunks
    }
}

impl<T> ComplexWrite<T> {
    pub fn almost_ready(&self) -> bool {
        RefCell::borrow(&self.proxy).almost_ready()
    }
}

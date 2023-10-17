use std::{borrow::BorrowMut, cell::RefCell, marker::PhantomData, rc::Rc};

use derive_more::Constructor;
use enum_dispatch::enum_dispatch;
use fxhash::FxHashSet;

#[enum_dispatch]
pub trait AccessLike {
    /// Updates the access and returns (updated, ready)
    fn update(&mut self, addr: u64) -> (bool, bool);
    fn get_addr(&self) -> u64;
    fn is_write(&self) -> bool;
}

#[enum_dispatch(AccessLike)]
#[derive(Clone)]
pub enum Access<T> {
    SimpleRead(SimpleRead),
    SimpleWrite(SimpleWrite<T>),

    ComplexRead(ComplexRead),
    ComplexWrite(ComplexWrite<T>),
}

#[derive(Clone, Copy)]
pub struct SimpleRead {
    base: u64,
}

impl AccessLike for SimpleRead {
    fn update(&mut self, addr: u64) -> (bool, bool) {
        let resp = addr == self.base;
        (resp, resp)
    }
    fn get_addr(&self) -> u64 {
        self.base
    }

    fn is_write(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct SimpleWrite<T> {
    base: u64,
    payload: T,
}

impl<T> AccessLike for SimpleWrite<T> {
    fn update(&mut self, addr: u64) -> (bool, bool) {
        let resp = addr == self.base;
        (resp, resp)
    }

    fn get_addr(&self) -> u64 {
        self.base
    }

    fn is_write(&self) -> bool {
        true
    }
}

struct ComplexAccessData {
    accesses: FxHashSet<u64>,
    base: u64,
}

#[derive(Clone)]
struct ComplexAccessProxy {
    data: Rc<RefCell<ComplexAccessData>>,
}

impl ComplexAccessProxy {
    fn update(&mut self, addr: u64) -> (bool, bool) {
        RefCell::borrow_mut(&self.data).update(addr)
    }
}

#[derive(Clone)]
pub struct ComplexRead {
    proxy: ComplexAccessProxy,
    offset: u64,
}

impl ComplexAccessData {
    fn update(&mut self, addr: u64) -> (bool, bool) {
        let updated = self.accesses.remove(&addr);
        (updated, self.accesses.is_empty())
    }
}

impl AccessLike for ComplexRead {
    fn update(&mut self, addr: u64) -> (bool, bool) {
        self.proxy.update(addr)
    }

    fn get_addr(&self) -> u64 {
        self.offset + self.proxy.data.borrow().base
    }

    fn is_write(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct ComplexWrite<T> {
    proxy: ComplexAccessProxy,
    offset: u64,
    payload: Rc<T>,
}

impl<T> AccessLike for ComplexWrite<T> {
    fn update(&mut self, addr: u64) -> (bool, bool) {
        self.proxy.update(addr)
    }

    fn get_addr(&self) -> u64 {
        self.offset + self.proxy.data.borrow().base
    }

    fn is_write(&self) -> bool {
        true
    }
}

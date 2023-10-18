use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use access::{
    Access, AccessLike, ComplexAccessData, ComplexAccessProxy, ComplexRead, ComplexWrite,
    SimpleRead, SimpleWrite,
};
use address::ChunkAddress;
use chunks::Chunk;
use dam::channel::{DequeueError, PeekResult};
use dam::types::IndexLike;
use dam::{context_tools::*, types::StaticallySized};
use derive_more::Constructor;

pub use ramulator_wrapper;
use request_manager::RequestManager;
use utils::VecUtils;

mod access;
mod address;
mod chunks;
mod request_manager;
mod utils;

type Recv<T> = dyn dam::channel::adapters::RecvAdapter<T> + Sync + Send;
type Snd<T> = dyn dam::channel::adapters::SendAdapter<T> + Sync + Send;

/// A wrapper around ramulator_wrapper (around Ramulator) which converts it into a context.
#[context_macro]
pub struct RamulatorContext<T: Clone> {
    ramulator: ramulator_wrapper::RamulatorWrapper,
    bytes_per_access: u64,

    // Elapsed cycles is measured w.r.t. the memory clock, which isn't necessarily the same as the global 'tick'
    cycles_per_tick: (num_bigint::BigUint, num_bigint::BigUint),
    elapsed_cycles: num_bigint::BigUint,

    // Stores chunks at a time, so conversion between byte-address and chunk-address requires dividing by bytes_per_access
    datastore: Vec<Chunk<T>>,

    writers: Vec<WriteBundle<T>>,
    readers: Vec<ReadBundle<T>>,
}

/// A WriteBundle consists of:
///   1. A Data channel, which contains things that can be decomposed into 'chunks' of size T.
///   2. An address channel containing the base address (in bytes)
///   3. An acknowledgement channel which is written at the end of the access
#[derive(Constructor)]
pub struct WriteBundle<T> {
    data: Box<Recv<T>>,
    addr: Box<Recv<u64>>,
    ack: Box<Snd<bool>>,
}

/// A ReadBundle consists of:
///   1. An address channel containing the base address (in bytes)
///   2. A size channel containing the read size in bytes
///   3. A response channel containing the data read out.
#[derive(Constructor)]
pub struct ReadBundle<T> {
    addr: Box<Recv<u64>>,
    size: Box<Recv<u64>>,
    resp: Box<Snd<T>>,
}

impl<T: DAMType> Context for RamulatorContext<T> {
    fn run(&mut self) {
        // by pulling this out, Access doesn't need to be Send/Sync.
        let mut backlog: Vec<Access<T>> = vec![];

        let mut request_manager = RequestManager::<T>::default();

        while self.continue_running(&backlog, &request_manager) {
            // Process the existing active requests
            // Try to process existing returns, at most one at a time.
            while self.ramulator.ret_available() {
                let resp_loc = address::ByteAddress(self.ramulator.pop());
                let chunk_addr = resp_loc.to_chunk(self.bytes_per_access);
                match request_manager.register_recv(resp_loc) {
                    Access::SimpleRead(read) => {
                        let data = self.read_chunk(chunk_addr).unwrap().clone();
                        self.readers[read.bundle_index()]
                            .resp
                            .enqueue(
                                &self.time,
                                ChannelElement {
                                    time: self.time.tick() + 1,
                                    data,
                                },
                            )
                            .unwrap();
                        break;
                    }
                    Access::SimpleWrite(write) => {
                        let index = write.bundle_index();
                        let data = write.payload;
                        self.write_chunk(chunk_addr, Chunk::First(data));
                        self.writers[index]
                            .ack
                            .enqueue(
                                &self.time,
                                ChannelElement {
                                    time: self.time.tick() + 1,
                                    data: true,
                                },
                            )
                            .unwrap();
                        break;
                    }
                    Access::ComplexRead(read) if read.is_ready() => {
                        let data = self.read_chunk(chunk_addr).unwrap().clone();
                        // Sweep forward, checking to see if they're marked as blank
                        for i in 1..read.num_chunks() {
                            assert!(self.read_chunk(chunk_addr + i).is_blank());
                        }

                        self.readers[read.bundle_index()]
                            .resp
                            .enqueue(
                                &self.time,
                                ChannelElement {
                                    time: self.time.tick() + 1,
                                    data,
                                },
                            )
                            .unwrap();
                        break;
                    }

                    Access::ComplexRead(_) => {
                        // Not ready yet
                    }

                    Access::ComplexWrite(write) if write.is_ready() => {
                        let index = write.bundle_index();
                        let num_chunks = write.num_chunks();
                        let data = write.payload;
                        self.write_chunk(chunk_addr, Chunk::First(Rc::into_inner(data).expect("Should not have resolved a multi-chunk write before all chunks are resolved.")));
                        // Sweep forward, checking to see if they're marked as blank
                        for i in 1..num_chunks {
                            self.write_chunk(chunk_addr + i, Chunk::Blank);
                        }
                        self.writers[index]
                            .ack
                            .enqueue(
                                &self.time,
                                ChannelElement {
                                    time: self.time.tick() + 1,
                                    data: true,
                                },
                            )
                            .unwrap();
                    }

                    Access::ComplexWrite(_) => {
                        // Not ready yet
                    }
                }
            }

            // In case we had to stall on the enqueue side.
            self.update_ticks();

            // Try to move elements from backlog to active.
            // The backlog isn't really a 'real' concept, but rather to support "large" objects which:
            //   1. span more than a single access chunk
            //   2. can't all be issued together.
            backlog.filter_swap_retain(
                |access| {
                    let is_ready = self
                        .ramulator
                        .available(access.get_addr().0, access.is_write());
                    !is_ready
                },
                |access| request_manager.add_request(access),
            );

            // Since the backlog is empty, we try to process more requests.
            // For each writer / reader, try to add it to the queue.
            if backlog.is_empty() {
                self.update_write_requests(&mut request_manager, &mut backlog);
            }

            if backlog.is_empty() {
                self.update_read_requests(&mut request_manager, &mut backlog);
            }

            self.time.incr_cycles(1);
            self.update_ticks();
        }
    }
}

impl<T: DAMType> RamulatorContext<T> {
    // Requires a special handler for converting global "tick" count to local cycle count.
    fn update_ticks(&mut self) {
        let cur_ticks = self.time.tick().time();
        let expected_cycles = (cur_ticks * &self.cycles_per_tick.0) / &self.cycles_per_tick.1;
        while self.elapsed_cycles < expected_cycles {
            self.elapsed_cycles += 1u32;
            self.ramulator.cycle();
        }
    }

    fn num_chunks(&self, obj_size_in_bytes: u64) -> u64 {
        assert_ne!(obj_size_in_bytes, 0);
        (self.bytes_per_access + obj_size_in_bytes - 1) / self.bytes_per_access
    }

    fn update_write_requests(
        &mut self,
        request_manager: &mut RequestManager<T>,
        backlog: &mut Vec<Access<T>>,
    ) {
        let cur_time = self.time.tick();
        for (ind, writer) in self.writers.iter().enumerate() {
            match (writer.addr.peek(), writer.data.peek()) {
                (
                    PeekResult::Something(ChannelElement {
                        time: addr_time,
                        data: addr_data,
                    }),
                    PeekResult::Something(ChannelElement {
                        time: data_time,
                        data,
                    }),
                ) => {
                    if addr_time <= cur_time && data_time <= cur_time {
                        let base_address = addr_data.try_into().unwrap();
                        let data_size_in_bytes = data.dam_size() as u64 / 8;
                        if data_size_in_bytes > self.bytes_per_access {
                            // Multiple requests are necessary.

                            let proxy = ComplexAccessProxy::new(Rc::new(RefCell::new(
                                ComplexAccessData::new(
                                    base_address,
                                    data_size_in_bytes,
                                    self.num_chunks(data_size_in_bytes),
                                    0,
                                    ind,
                                ),
                            )));
                            let value = Rc::new(data);

                            {
                                let mut offset_in_bytes = 0;
                                while offset_in_bytes < data_size_in_bytes {
                                    let access = ComplexWrite::new(
                                        proxy.clone(),
                                        offset_in_bytes,
                                        value.clone(),
                                    )
                                    .into();
                                    Self::enqueue_or_backlog(
                                        &mut self.ramulator,
                                        access,
                                        request_manager,
                                        backlog,
                                    );

                                    offset_in_bytes += self.bytes_per_access;
                                }
                            }
                        } else {
                            let access = SimpleWrite::new(base_address, data, ind).into();
                            Self::enqueue_or_backlog(
                                &mut self.ramulator,
                                access,
                                request_manager,
                                backlog,
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn update_read_requests(
        &mut self,
        request_manager: &mut RequestManager<T>,
        backlog: &mut Vec<Access<T>>,
    ) {
        let cur_time = self.time.tick();
        for (ind, reader) in self.readers.iter().enumerate() {
            match (reader.addr.peek(), reader.size.peek()) {
                (
                    PeekResult::Something(ChannelElement { time, data: addr }),
                    PeekResult::Something(ChannelElement {
                        time: size_time,
                        data: read_size_in_bytes,
                    }),
                ) if time <= cur_time && size_time <= cur_time => {
                    let base_address = addr.try_into().unwrap();

                    if read_size_in_bytes > self.bytes_per_access {
                        // Stores all of the sub-addresses that need to be processed before this one.
                        let proxy =
                            ComplexAccessProxy::new(Rc::new(RefCell::new(ComplexAccessData::new(
                                base_address,
                                read_size_in_bytes,
                                self.num_chunks(read_size_in_bytes),
                                0,
                                ind,
                            ))));

                        {
                            let mut offset_in_bytes = 0;
                            while offset_in_bytes < read_size_in_bytes {
                                let access =
                                    ComplexRead::new(proxy.clone(), offset_in_bytes).into();
                                Self::enqueue_or_backlog(
                                    &mut self.ramulator,
                                    access,
                                    request_manager,
                                    backlog,
                                );
                                offset_in_bytes += self.bytes_per_access;
                            }
                        }
                    } else {
                        let access = SimpleRead::new(base_address, ind).into();
                        Self::enqueue_or_backlog(
                            &mut self.ramulator,
                            access,
                            request_manager,
                            backlog,
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn enqueue_or_backlog(
        ramulator: &mut ramulator_wrapper::RamulatorWrapper,
        access: Access<T>,
        manager: &mut RequestManager<T>,
        backlog: &mut Vec<Access<T>>,
    ) {
        if ramulator.available(access.get_addr().into(), access.is_write()) {
            ramulator.send(access.get_addr().into(), access.is_write());
            manager.add_request(access);
        } else {
            backlog.push(access);
        }
    }

    fn read_chunk(&self, addr: ChunkAddress) -> &Chunk<T> {
        &self.datastore[addr.0 as usize]
    }

    fn write_chunk(&mut self, addr: ChunkAddress, data: Chunk<T>) {
        self.datastore[addr.0 as usize] = data;
    }

    fn continue_running(
        &mut self,
        backlog: &Vec<Access<T>>,
        request_manager: &RequestManager<T>,
    ) -> bool {
        if !backlog.is_empty() {
            return true;
        }

        if !request_manager.is_empty() {
            return true;
        }

        // check all of the writers
        let writers_done = self
            .writers
            .iter()
            .all(
                |WriteBundle { data, addr, ack: _ }| match (data.peek(), addr.peek()) {
                    (PeekResult::Closed, _) | (_, PeekResult::Closed) => true,
                    _ => false,
                },
            );

        if !writers_done {
            return true;
        }

        let readers_done = self.readers.iter().all(
            |ReadBundle {
                 addr,
                 size,
                 resp: _,
             }| {
                match (addr.peek(), size.peek()) {
                    (PeekResult::Closed, _) | (_, PeekResult::Closed) => true,
                    _ => false,
                }
            },
        );

        if !readers_done {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn simple_ramulator() {
        use ramulator_wrapper::RamulatorWrapper;
        use std::collections::HashSet;
        let mut ramulator =
            RamulatorWrapper::new_with_preset(ramulator_wrapper::PresetConfigs::DDR4, "test2.txt");
        let mut cycle = 0;
        let count = 10u64;
        let mut all_req: HashSet<_> = (1..count).into_iter().map(|i| i * 64).collect();
        for i in 1..count {
            while !ramulator.available(i * 64, false) {
                ramulator.cycle();
                cycle += 1;
            }
            ramulator.send(i * 64, false);
            println!("Send: {}", i * 64);
            ramulator.cycle();
        }
        for _i in 1..count {
            while !ramulator.ret_available() {
                ramulator.cycle();
                cycle += 1;
            }
            let result = ramulator.pop();
            ramulator.cycle();

            //assert!(all_req.contains(&result));
            println!("Recv: {}", result);
            all_req.remove(&result);
        }
        for _i in 0..1000 {
            ramulator.cycle();
        }
        assert_eq!(ramulator.ret_available(), false);

        println!("cycle: {}", cycle);
    }
}

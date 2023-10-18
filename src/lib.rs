use std::cell::RefCell;

use std::rc::Rc;

use access::{
    Access, AccessLike, ComplexAccessData, ComplexAccessProxy, ComplexRead, ComplexWrite,
    SimpleRead, SimpleWrite,
};
use address::ChunkAddress;
use chunks::Chunk;
use dam::channel::PeekResult;

use dam::context_tools::*;
use derive_more::Constructor;

use num_bigint::BigUint;
pub use ramulator_wrapper;
use request_manager::RequestManager;
use utils::VecUtils;

use crate::access::Read;

mod access;
mod address;
mod chunks;
mod request_manager;
mod utils;

type Recv<'a, T> = dyn dam::channel::adapters::RecvAdapter<T> + Sync + Send + 'a;
type Snd<'a, T> = dyn dam::channel::adapters::SendAdapter<T> + Sync + Send + 'a;

/// A wrapper around ramulator_wrapper (around Ramulator) which converts it into a context.
#[context_macro]
pub struct RamulatorContext<'a, T: Clone> {
    ramulator: ramulator_wrapper::RamulatorWrapper,
    bytes_per_access: u64,

    // Elapsed cycles is measured w.r.t. the memory clock, which isn't necessarily the same as the global 'tick'
    cycles_per_tick: (num_bigint::BigUint, num_bigint::BigUint),
    elapsed_cycles: num_bigint::BigUint,

    // Stores chunks at a time, so conversion between byte-address and chunk-address requires dividing by bytes_per_access
    datastore: Vec<Chunk<T>>,

    writers: Vec<WriteBundle<'a, T>>,
    readers: Vec<ReadBundle<'a, T>>,
}

/// A WriteBundle consists of:
///   1. A Data channel, which contains things that can be decomposed into 'chunks' of size T.
///   2. An address channel containing the base address (in bytes)
///   3. An acknowledgement channel which is written at the end of the access
#[derive(Constructor)]
pub struct WriteBundle<'a, T> {
    data: Box<Recv<'a, T>>,
    addr: Box<Recv<'a, u64>>,
    ack: Box<Snd<'a, bool>>,
}

/// A ReadBundle consists of:
///   1. An address channel containing the base address (in bytes)
///   2. A size channel containing the read size in bytes
///   3. A response channel containing the data read out.
#[derive(Constructor)]
pub struct ReadBundle<'a, T> {
    addr: Box<Recv<'a, u64>>,
    size: Box<Recv<'a, u64>>,
    resp: Box<Snd<'a, T>>,
    resp_addr: Box<Snd<'a, u64>>,
}

impl<T: DAMType> Context for RamulatorContext<'_, T> {
    fn run(&mut self) {
        // by pulling this out, Access doesn't need to be Send/Sync.
        let mut backlog: Vec<Access<T>> = vec![];

        // This handles reads; writes are automatically handled if ready.
        let mut request_manager = RequestManager::default();

        while self.continue_running(&backlog, &request_manager) {
            // Process the existing active requests
            // Try to process existing returns, at most one at a time.
            while self.ramulator.ret_available() {
                let resp_loc = address::ByteAddress(self.ramulator.pop());
                let chunk_addr = resp_loc.to_chunk(self.bytes_per_access);
                match request_manager.register_recv(resp_loc) {
                    Read::SimpleRead(read) => {
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
                        self.readers[read.bundle_index()]
                            .resp_addr
                            .enqueue(
                                &self.time,
                                ChannelElement {
                                    time: self.time.tick() + 1,
                                    data: resp_loc.0,
                                },
                            )
                            .unwrap();
                        break;
                    }

                    Read::ComplexRead(read) if read.almost_ready() => {
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

                        self.readers[read.bundle_index()]
                            .resp_addr
                            .enqueue(
                                &self.time,
                                ChannelElement {
                                    time: self.time.tick() + 1,
                                    data: read.base_addr().0,
                                },
                            )
                            .unwrap();
                        break;
                    }

                    Read::ComplexRead(_) => {
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
            let mut to_process = vec![];
            backlog.filter_swap_retain(
                |access| {
                    let is_ready = self
                        .ramulator
                        .available(access.get_addr().0, access.is_write());
                    !is_ready
                },
                |access| to_process.push(access),
            );

            to_process.into_iter().for_each(|access| {
                self.enqueue_or_backlog(access, &mut request_manager, &mut backlog);
            });

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

impl<'a, T: DAMType> RamulatorContext<'a, T> {
    pub fn new<A, B>(
        ramulator: ramulator_wrapper::RamulatorWrapper,
        bytes_per_access: u64,
        cycles_per_tick: (A, B),
        datastore: Vec<Chunk<T>>,
    ) -> Self
    where
        A: Into<BigUint>,
        B: Into<BigUint>,
    {
        Self {
            ramulator,
            bytes_per_access,
            cycles_per_tick: (cycles_per_tick.0.into(), cycles_per_tick.1.into()),
            elapsed_cycles: 0u32.into(),
            datastore,
            writers: vec![],
            readers: vec![],
            context_info: Default::default(),
        }
    }

    pub fn add_reader(
        &mut self,
        ReadBundle {
            addr,
            size,
            resp,
            resp_addr,
        }: ReadBundle<'a, T>,
    ) {
        addr.attach_receiver(self);
        size.attach_receiver(self);
        resp.attach_sender(self);
        resp_addr.attach_sender(self);
        self.readers.push(ReadBundle {
            addr,
            size,
            resp,
            resp_addr,
        });
    }

    pub fn add_writer(&mut self, WriteBundle { data, addr, ack }: WriteBundle<'a, T>) {
        data.attach_receiver(self);
        addr.attach_receiver(self);
        ack.attach_sender(self);
        self.writers.push(WriteBundle { data, addr, ack })
    }

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
        request_manager: &mut RequestManager,
        backlog: &mut Vec<Access<T>>,
    ) {
        let cur_time = self.time.tick();
        let mut accesses = vec![];
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
                        // Pop the peeked values
                        writer.addr.dequeue(&self.time).unwrap();
                        writer.data.dequeue(&self.time).unwrap();
                        let base_address = addr_data.try_into().unwrap();
                        let data_size_in_bytes = data.dam_size() as u64 / 8;
                        if data_size_in_bytes > self.bytes_per_access {
                            // Multiple requests are necessary.

                            let proxy = ComplexAccessProxy::new(Rc::new(RefCell::new(
                                ComplexAccessData::new(
                                    base_address,
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
                                    // self.enqueue_or_backlog(access, request_manager, backlog);
                                    accesses.push(access);

                                    offset_in_bytes += self.bytes_per_access;
                                }
                            }
                        } else {
                            let access = SimpleWrite::new(base_address, data, ind).into();
                            // self.enqueue_or_backlog(access, request_manager, backlog);
                            accesses.push(access);
                        }
                    }
                }
                _ => {}
            }
        }
        for access in accesses {
            self.enqueue_or_backlog(access, request_manager, backlog)
        }
    }

    fn update_read_requests(
        &mut self,
        request_manager: &mut RequestManager,
        backlog: &mut Vec<Access<T>>,
    ) {
        let cur_time = self.time.tick();
        let mut accesses: Vec<Access<T>> = vec![];
        for (ind, reader) in self.readers.iter().enumerate() {
            match (reader.addr.peek(), reader.size.peek()) {
                (
                    PeekResult::Something(ChannelElement { time, data: addr }),
                    PeekResult::Something(ChannelElement {
                        time: size_time,
                        data: read_size_in_bytes,
                    }),
                ) if time <= cur_time && size_time <= cur_time => {
                    // Pop the peeked values
                    reader.addr.dequeue(&self.time).unwrap();
                    reader.size.dequeue(&self.time).unwrap();

                    let base_address = addr.try_into().unwrap();

                    if read_size_in_bytes > self.bytes_per_access {
                        // Stores all of the sub-addresses that need to be processed before this one.
                        let proxy =
                            ComplexAccessProxy::new(Rc::new(RefCell::new(ComplexAccessData::new(
                                base_address,
                                self.num_chunks(read_size_in_bytes),
                                0,
                                ind,
                            ))));

                        {
                            let mut offset_in_bytes = 0;
                            while offset_in_bytes < read_size_in_bytes {
                                let access =
                                    ComplexRead::new(proxy.clone(), offset_in_bytes).into();
                                // self.enqueue_or_backlog(access, request_manager, backlog);\
                                accesses.push(access);
                                offset_in_bytes += self.bytes_per_access;
                            }
                        }
                    } else {
                        let access = SimpleRead::new(base_address, ind).into();
                        accesses.push(access);
                        // self.enqueue_or_backlog(access, request_manager, backlog);
                    }
                }
                _ => {}
            }
        }
        for access in accesses {
            self.enqueue_or_backlog(access, request_manager, backlog)
        }
    }

    fn enqueue_or_backlog(
        &mut self,
        access: Access<T>,
        manager: &mut RequestManager,
        backlog: &mut Vec<Access<T>>,
    ) {
        if self
            .ramulator
            .available(access.get_addr().into(), access.is_write())
        {
            self.ramulator
                .send(access.get_addr().into(), access.is_write());
            match access {
                Access::SimpleRead(rd) => manager.add_request(rd.into()),
                Access::ComplexRead(rd) => manager.add_request(rd.into()),
                Access::SimpleWrite(write) => {
                    let chunk_addr = write.get_addr().to_chunk(self.bytes_per_access);
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
                }
                Access::ComplexWrite(write) if write.almost_ready() => {
                    let chunk_addr = write.get_addr().to_chunk(self.bytes_per_access);
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

                Access::ComplexWrite(mut wr) => {
                    wr.mark_resolved();
                }
            }
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
        request_manager: &RequestManager,
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
                 resp_addr: _,
             }| {
                match (addr.peek(), size.peek()) {
                    (PeekResult::Closed, _) | (_, PeekResult::Closed) => true,
                    _ => false,
                }
            },
        );

        if !readers_done {
            println!("Readers Nonempty");
            return true;
        }

        false
    }
}

#[cfg(test)]
mod test {

    use dam::context_tools::*;
    use dam::simulation::{InitializationOptions, ProgramBuilder, RunOptions};
    use dam::utility_contexts::*;
    use ramulator_wrapper::RamulatorWrapper;

    use crate::chunks::Chunk;
    use crate::RamulatorContext;
    #[test]
    fn ramulator_e2e_small() {
        const MEM_SIZE: usize = 32;

        let mut parent = ProgramBuilder::default();
        let ramulator =
            RamulatorWrapper::new_with_preset(ramulator_wrapper::PresetConfigs::DDR4, "test.txt");

        let datastore: Vec<Chunk<u32>> = vec![Default::default(); MEM_SIZE];

        let mut mem_context = RamulatorContext::new(ramulator, 64, (1u32, 1u32), datastore);

        let (addr_snd, addr_rcv) = parent.unbounded();
        let (data_snd, data_rcv) = parent.unbounded();
        let (ack_snd, ack_rcv) = parent.unbounded::<bool>();
        let addrs = || (0..(MEM_SIZE as u64)).map(|x| x * 64);
        parent.add_child(GeneratorContext::new(addrs, addr_snd));
        parent.add_child(GeneratorContext::new(|| 1..(1 + MEM_SIZE as u64), data_snd));

        mem_context.add_writer(crate::WriteBundle {
            data: Box::new(data_rcv),
            addr: Box::new(addr_rcv),
            ack: Box::new(ack_snd),
        });

        let (raddr_snd, raddr_rcv) = parent.unbounded();
        let (rdata_snd, rdata_rcv) = parent.unbounded::<u64>();
        let (size_snd, size_rcv) = parent.unbounded();

        let mut read_ctx = FunctionContext::new();
        raddr_snd.attach_sender(&read_ctx);
        size_snd.attach_sender(&read_ctx);
        ack_rcv.attach_receiver(&read_ctx);
        read_ctx.set_run(move |time| {
            for iter in 0..MEM_SIZE {
                // Wait for an ack to be received
                ack_rcv.dequeue(time).unwrap();

                raddr_snd
                    .enqueue(
                        time,
                        ChannelElement {
                            time: time.tick() + 1,
                            data: 64 * (iter as u64),
                        },
                    )
                    .unwrap();
                size_snd
                    .enqueue(
                        time,
                        ChannelElement {
                            time: time.tick() + 1,
                            // Going to be super inefficient and only use 8 bytes (64 bits) instead of the full 64 byte access
                            data: 8u64,
                        },
                    )
                    .unwrap();
            }
        });
        parent.add_child(read_ctx);

        let (resp_addr_snd, resp_addr_rcv) = parent.unbounded::<u64>();
        mem_context.add_reader(crate::ReadBundle {
            addr: Box::new(raddr_rcv),
            size: Box::new(size_rcv),
            resp: Box::new(rdata_snd),
            resp_addr: Box::new(resp_addr_snd),
        });

        parent.add_child(mem_context);
        let mut verif_context = FunctionContext::new();
        resp_addr_rcv.attach_receiver(&verif_context);
        rdata_rcv.attach_receiver(&verif_context);
        verif_context.set_run(move |time| {
            let mut received: fxhash::FxHashMap<u64, u64> = Default::default();
            for _ in 0..MEM_SIZE {
                let addr = resp_addr_rcv.dequeue(time).unwrap().data;
                let data = rdata_rcv.dequeue(time).unwrap().data;
                received.insert(addr, data);
                time.incr_cycles(1);
            }
            println!("Received: {:?}", received);
        });

        parent.add_child(verif_context);

        println!("Finished building");

        parent
            .initialize(InitializationOptions::default())
            .unwrap()
            .run(RunOptions::default());
    }

    #[test]
    fn simple_ramulator() {
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

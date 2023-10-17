use std::collections::VecDeque;

use access::{Access, AccessLike};
use dam::channel::{DequeueError, PeekResult};
use dam::types::IndexLike;
use dam::{context_tools::*, types::StaticallySized};
use derive_more::Constructor;

pub use ramulator_wrapper;
use utils::VecUtils;

mod access;
mod chunks;
mod utils;

type Recv<T> = dyn dam::channel::adapters::RecvAdapter<T> + Sync + Send;
type Snd<T> = dyn dam::channel::adapters::SendAdapter<T> + Sync + Send;

/// A wrapper around ramulator_wrapper (around Ramulator) which converts it into a context.
#[context_macro]
pub struct RamulatorContext<T: Clone> {
    ramulator: ramulator_wrapper::RamulatorWrapper,
    bytes_per_access: usize,

    // Elapsed cycles is measured w.r.t. the memory clock, which isn't necessarily the same as the global 'tick'
    cycles_per_tick: (num_bigint::BigUint, num_bigint::BigUint),
    elapsed_cycles: num_bigint::BigUint,

    datastore: Vec<Option<T>>,

    writers: Vec<WriteBundle<T>>,
    readers: Vec<ReadBundle<T>>,
}

/// A WriteBundle consists of:
///   1. A Data channel, which contains things that can be decomposed into 'chunks' of size T.
///   2. An address channel containing the base address
///   3. An acknowledgement channel which is written at the end of the access
#[derive(Constructor)]
pub struct WriteBundle<T> {
    data: Box<Recv<T>>,
    addr: Box<Recv<u64>>,
    ack: Box<Snd<bool>>,
}

/// A ReadBundle consists of:
///   1. An address channel containing the base address
///   2. A size channel containing the number of elements to read
///   3. A response channel containing the data read out.
#[derive(Constructor)]
pub struct ReadBundle<T> {
    addr: Box<Recv<u64>>,
    size: Box<Recv<u64>>,
    resp: Box<Snd<T>>,
}

type RequestQueueType<T> = fxhash::FxHashMap<u64, VecDeque<Access<T>>>;

impl<T: DAMType + StaticallySized> Context for RamulatorContext<T> {
    fn run(&mut self) {
        // by pulling this out, Access doesn't need to be Send/Sync.
        let mut backlog: Vec<Access<T>> = vec![];
        let mut active: Vec<Access<T>> = vec![];

        loop {
            // Process the existing active requests

            // Try to process existing returns
            if self.ramulator.ret_available() {
                let resp_loc = self.ramulator.pop();

                for acc in &mut active {
                    let (updated, ready) = acc.update(resp_loc);
                    if updated {
                        if ready {
                            // Process the access
                            todo!();
                        }
                        break;
                    }
                }

                let mut found = false;
                active.filter_swap_retain(
                    |access| {
                        if found {
                            return true;
                        }

                        let (updated, ready) = access.update(resp_loc);

                        if updated {
                            found = true;
                            !ready
                        } else {
                            true
                        }
                    },
                    |access| todo!("Process ready accesss"),
                )
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
                        .available(access.get_addr(), access.is_write());
                    !is_ready
                },
                |access| active.push(access),
            );

            if backlog.is_empty() {
                // Since the backlog is empty, we try to process more requests.
            }
        }
    }
}

impl<T: DAMType + StaticallySized> RamulatorContext<T> {
    // If there is an active request
    fn iter_with_active_req(&mut self) {
        if self.ramulator.ret_available() {}
    }

    // If there isn't currently an active request
    fn iter_without_active_req(&mut self) {
        // Without any active requests, we advance forward in time until there is a ready event first.
        self.advance_until_request_is_active();
        self.get_new_requests();
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

    fn advance_until_request_is_active(&self) {
        todo!();
    }

    /// Gets as many new requests as possible, pushing them into ramulator.
    fn get_new_requests(&mut self) {
        let cur_time = self.time.tick();
        // For each writer / reader, try to add it to the queue.
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
                        let address = addr_data.try_into().unwrap();
                        if self.ramulator.available(address, true) {
                            self.ramulator.send(address, true);
                            // active_requests
                            //     .entry(address)
                            //     .or_default()
                            //     .push_back(Access::Write(ind, data));
                        }
                    }
                }
                _ => {}
            }
        }
        for (ind, reader) in self.readers.iter().enumerate() {
            match (reader.addr.peek(), reader.size.peek()) {
                (
                    PeekResult::Something(ChannelElement { time, data: addr }),
                    PeekResult::Something(ChannelElement {
                        time: size_time,
                        data: read_size,
                    }),
                ) if time <= cur_time && size_time <= cur_time => {
                    let address = addr.try_into().unwrap();
                    if self.ramulator.available(address, false) {
                        self.ramulator.send(address, false);
                        // Register the read into active requests.
                        // active_requests
                        //     .entry(address)
                        //     .or_default()
                        //     .push_back(Access::Read(ind, Vec::with_capacity(read_size)));
                    }
                }
                _ => {}
            }
        }
    }

    fn drop_dead_accesses(&mut self) {
        self.writers
            .retain(|writer| match (writer.addr.peek(), writer.data.peek()) {
                (PeekResult::Closed, PeekResult::Closed) => false,
                _ => true,
            });
        self.readers.retain(|reader| {
            if let PeekResult::Closed = reader.addr.peek() {
                false
            } else {
                true
            }
        });
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

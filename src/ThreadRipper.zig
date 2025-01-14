const std = @import("std");
const print = std.debug.print;
const builtin = @import("builtin");
const Atomic = std.atomic.Value;
const assert = std.debug.assert;

// This is the ThreadRipper
const ThreadRipper = @This();
arena: *std.mem.Allocator,
sync: Atomic(u32) = Atomic(u32).init(@bitCast(Sync{})),
workingCount: Atomic(u32) = Atomic(u32).init(0),
stack_size: u32,
max_threads: u32,
global_queue: ContractQueue = .{},
threads_stack: Atomic(?*Thread) = Atomic(?*Thread).init(null),
idle_event: Event = .{},
join_event: Event = .{},

const TRError = error{
    FailedToInitThreadRipper,
};

pub const Options = struct {
    stack_size: u32 = (std.Thread.SpawnConfig{}).stack_size,
    arena: *std.mem.Allocator,
    max_threads: ?u32 = null,
};

const Job = struct {
    runFn: JobProto,
};

pub const JobList = struct {
    head: *Node,
    tail: *Node,
};

const JobProto = *const fn (*Job) anyerror!void;

pub const Node = struct {
    id: u32 = 0,
    data: Job,
    next: ?*Node = null,
};

pub const Sync = packed struct {
    /// Tracks the number of threads not searching for Tasks
    idle_threads: u14 = 0,
    /// Tracks the number of threadsspawned_threads
    spawned_threads: u14 = 0,
    /// What you see is what you get
    unused: bool = false,
    /// Used to not miss notifications while state = waking
    notified: bool = false,
    /// The current state of the thread pool
    state: enum(u2) {
        /// A notification can be issued to wake up a sleeping as the "waking thread".
        pending = 0,
        /// The state was notifiied with a signal. A thread is woken up.
        /// The first thread to transition to `waking` becomes the "waking thread".
        signaled,
        /// There is a "waking thread" among us.
        /// No other thread should be woken up until the waking thread transitions the state.
        waking,
        /// The thread pool was terminated. Start decremented `spawned_threads` so that it can be joined.
        shutdown,
    } = .pending,
};

pub fn init(target: *ThreadRipper, options: Options) !void {
    const thread_count = options.max_threads orelse 1;

    target.* = .{
        .stack_size = @max(1, options.stack_size),
        .max_threads = thread_count,
        .arena = options.arena,
    };

    // _ = target.workingCount.cmpxchgStrong(0, thread_count, .acquire, .monotonic);

    if (builtin.single_threaded) {
        return;
    }

    // kill and join any threads we spawned and free memory on error.
    // target.threads = try target.arena.alloc(std.Thread, thread_count);
    var spawned: u14 = 0;
    // errdefer target.join(spawned);

    var sync: Sync = @bitCast(target.sync.load(.monotonic));
    var new_sync = sync;

    for (0..thread_count) |_| {
        const spawn_config = std.Thread.SpawnConfig{ .stack_size = target.stack_size };
        const spawned_thread = std.Thread.spawn(spawn_config, Thread.worker, .{target}) catch return target.unregister(null);
        spawned_thread.detach();
        spawned += 1;
    }

    new_sync.spawned_threads = spawned;
    while (true) {
        new_sync.spawned_threads = spawned;
        sync = @bitCast(target.sync.cmpxchgStrong(
            @bitCast(sync),
            @bitCast(new_sync),
            .acquire,
            .monotonic,
        ) orelse break);
    }
}

pub fn deinit(thread_ripper: *ThreadRipper) void {
    thread_ripper.join(thread_ripper.threads.len); // kill and join all threads.
    thread_ripper.* = undefined;
}

pub fn generateJob(thread_ripper: *ThreadRipper, comptime func: anytype, args: anytype) !*Node {
    const job_node = try thread_ripper.createNode(func, args);
    return job_node;
}

fn register(noalias thread_ripper: *ThreadRipper, noalias thread: *Thread) void {
    // Push the thread onto the threads stack in a lock-free manner.
    var threads_stack = thread_ripper.threads_stack.load(.monotonic);
    // Threads is a stack we add the thread to the front of the stack
    while (true) {
        thread.next = threads_stack;
        threads_stack = thread_ripper.threads_stack.cmpxchgStrong(
            threads_stack,
            thread,
            .release,
            .monotonic,
        ) orelse break;
    }
}
fn unregister(noalias thread_ripper: *ThreadRipper, noalias _: ?*Thread) void {
    // Un-spawn one thread, either due to a failed OS thread spawning or the thread is exitting.
    const one_spawned: u32 = @bitCast(Sync{ .spawned_threads = 1 });
    const sync: Sync = @bitCast(thread_ripper.sync.fetchSub(one_spawned, .release));
    assert(sync.spawned_threads > 0);

    // The last thread to exit must wake up the thread pool join()er
    // who will start the chain to shutdown all the threads.
    // if (sync.state == .shutdown and sync.spawned_threads == 1) {
    //     thread_ripper.join_event.notify();
    // }
    //
    // // If this is a thread pool thread, wait for a shutdown signal by the thread pool join()er.
    // const thread = maybe_thread orelse return;
    // thread.join_event.wait();
    //
    // // After receiving the shutdown signal, shutdown the next thread in the pool.
    // // We have to do that without touching the thread pool itself since it's memory is invalidated by now.
    // // So just follow our .next link.
    // const next_thread = thread.next orelse return;
    // next_thread.join_event.notify();
}

fn createNode(thread_ripper: *ThreadRipper, comptime func: anytype, args: anytype) !*Node {
    if (builtin.single_threaded) {
        try @call(.auto, func, args);
        return;
    }

    const Args = @TypeOf(args);
    const Closure = struct {
        arguments: Args,
        thread_ripper: *ThreadRipper,
        run_node: Node = .{ .data = .{ .runFn = runFn } },

        fn runFn(job: *Job) !void {
            const run_node: *Node = @fieldParentPtr("data", job);
            const closure: *@This() = @alignCast(@fieldParentPtr("run_node", run_node));
            try @call(.auto, func, closure.arguments);
            closure.thread_ripper.arena.destroy(closure);
        }
    };

    const closure = try thread_ripper.arena.create(Closure);
    closure.* = .{
        .arguments = args,
        .thread_ripper = thread_ripper,
    };
    return &closure.run_node;
}

pub fn dispatchJob(thread_ripper: *ThreadRipper, node: *Node) !void {
    // const sync: Sync = @bitCast(thread_ripper.sync.load(.monotonic));
    // print("\nthread idle count {d}\n\n", .{sync.idle_threads});
    // const node = try thread_ripper.generateJob(testThreadFunc, .{});
    var job_list = JobList{
        .head = node,
        .tail = node,
    };

    thread_ripper.global_queue.push(&job_list);

    // Notify waiting threads outside the lock to try and keep the critical section small.
    const is_waking = false;
    thread_ripper.notify(is_waking);
}

pub fn dispatchBatch(thread_ripper: *ThreadRipper, job_list: *JobList) void {
    thread_ripper.global_queue.push(job_list);
    const is_waking = false;
    thread_ripper.notify(is_waking);
}

pub fn working(thread_ripper: *ThreadRipper) bool {
    const workingCount = thread_ripper.workingCount.load(.acquire);
    if (workingCount > 0) {
        return true;
    } else {
        std.time.sleep(5_000_000);
        return false;
    }
}

fn join(thread_ripper: *ThreadRipper) void {
    // Wait for the thread pool to be shutdown() then for all threads to enter a joinable state
    thread_ripper.join_event.wait();
    const sync: Sync = @bitCast(thread_ripper.sync.load(.monotonic));
    assert(sync.state == .shutdown);
    assert(sync.spawned == 0);

    // If there are threads, start off the chain sending it the shutdown signal.
    // The thread receives the shutdown signal and sends it to the next thread, and the next..
    const thread = thread_ripper.threads.load(.Acquire) orelse return;
    thread.join_event.notify();
}

// This function checks per worker thread if it is notified or is idle
noinline fn wait(thread_ripper: *ThreadRipper, _is_waking: bool) error{Shutdown}!bool {
    var is_idle = false;
    var is_waking = _is_waking;
    var sync: Sync = @bitCast(thread_ripper.sync.load(.monotonic));

    while (true) {
        // we check the thread_ripper sync flag to check if we are in shutdown mode
        if (sync.state == .shutdown) return error.Shutdown;
        // if is_waking is true then we assert that the state is also waking
        if (is_waking) assert(sync.state == .waking);

        // Consume a notification made by notify().
        // If we are in a notified state we reset the sync state
        if (sync.notified) {
            // print("Sync notified is true......\n", .{});
            var new_sync = sync;
            new_sync.notified = false;
            // we set the number of idle_threads threads
            // we decrease since we are notified a new thread
            if (is_idle)
                new_sync.idle_threads -= 1;
            // we check the sync state since its atomic if its sync state is .signaled
            if (sync.state == .signaled)
                new_sync.state = .waking;

            // Acquire barrier synchronizes with notify()
            // to ensure that pushes to run queue are observed after wait() returns.
            // We update the new sync state if we succeed then we have reset the state and return is_waking
            // which if it is false we return a bool if the sync.state is .signaled
            sync = @bitCast(thread_ripper.sync.cmpxchgStrong(
                @bitCast(sync),
                @bitCast(new_sync),
                .acquire,
                .monotonic,
            ) orelse {
                return is_waking or (sync.state == .signaled);
            });

            // No notification to consume.
            // Mark this thread as idle_threads before sleeping on the idle_event.
        } else if (!is_idle) {
            // so if the idle_threads is false
            var new_sync = sync;
            // we increment the idle_threads number of threads
            new_sync.idle_threads += 1;
            if (is_waking)
                new_sync.state = .pending;

            // if we succed we set is_waking to false and is_idle to true so now we update the loop
            sync = @bitCast(thread_ripper.sync.cmpxchgStrong(
                @bitCast(sync),
                @bitCast(new_sync),
                .monotonic,
                .monotonic,
            ) orelse {
                // print("CAS YES {d}", .{new_sync.idle_threads});
                is_waking = false;
                is_idle = true;
                continue;
            });

            // Wait for a signal by either notify() or shutdown() without wasting cpu cycles.
            // TODO: Add I/O polling here.
        } else {
            // if we are not notified and idle_threads = true
            thread_ripper.idle_event.wait();
            sync = @bitCast(thread_ripper.sync.load(.monotonic));
        }
    }
}

inline fn notify(thread_ripper: *ThreadRipper, is_waking: bool) void {
    // Fast path to check the Sync state to avoid calling into notifySlow().
    // If we're waking, then we need to update the state regardless
    // false then no thread is waking up atm
    if (!is_waking) {
        // we load the sync and check if notified
        // if notified then we return since we do not notify twice
        const sync: Sync = @bitCast(thread_ripper.sync.load(.monotonic));
        if (sync.notified) {
            return;
        }
    }

    // else we run notifySlow which essentially checks the threads and wakes them up appropriatley
    return thread_ripper.notifySlow(is_waking);
}

noinline fn notifySlow(thread_ripper: *ThreadRipper, is_waking: bool) void {
    // We load the new Sync state
    var sync: Sync = @bitCast(thread_ripper.sync.load(.monotonic));
    while (sync.state != .shutdown) {
        // we check if we can wake up a thread if is_waking or the state is pending
        const can_wake = is_waking or (sync.state == .pending);
        // then we need to make sure that if we are is_waking then the state also should be waking
        if (is_waking) {
            assert(sync.state == .waking);
        }

        var new_sync = sync;
        new_sync.notified = true;
        // we check that if we can wake that there are idle_threads threads
        if (can_wake and sync.idle_threads > 0) { // wake up an idle_threads thread
            // print("Idle threads: {d}\n", .{sync.idle_threads});
            new_sync.state = .signaled;
        } else if (can_wake and sync.spawned_threads < thread_ripper.max_threads) { // spawn a new thread
            // print("icrementing thread spawn count\n", .{});
            // we check if can_wake and the spawned threads is less than the max
            // threads can be waiting
            new_sync.state = .signaled;
            new_sync.spawned_threads += 1;
        } else if (is_waking) { // no other thread to pass on "waking" status
            // print("Is waking\n", .{});
            // there are no threads available to do work
            new_sync.state = .pending;
        } else if (sync.notified) { // nothing to update
            return;
        }

        // release barrier synchronizes with Acquire in wait()
        // to ensure pushes to run queues happen before observing a posted notification.
        sync = @bitCast(thread_ripper.sync.cmpxchgStrong(
            @bitCast(sync),
            @bitCast(new_sync),
            .release,
            .monotonic,
        ) orelse {
            // If we succedd in changing the state
            // We signaled to notify an idle_threads thread
            if (can_wake and sync.idle_threads > 0) {
                // print("CAS succeeded\n", .{});
                return thread_ripper.idle_event.notify();
            }

            // We signaled to spawn a new thread
            if (can_wake and sync.spawned_threads < thread_ripper.max_threads) {
                print("Spawning another thread\n", .{});
                const spawn_config = std.Thread.SpawnConfig{ .stack_size = thread_ripper.stack_size };
                const thread = std.Thread.spawn(spawn_config, Thread.worker, .{thread_ripper}) catch return thread_ripper.unregister(null);
                return thread.detach();
            }

            return;
        });
    }
}

const Thread = struct {
    next: ?*Thread = null,
    target: ?*Thread = null,
    join_event: Event = .{},
    job_queue: JobQueue = .{},
    contract_queue: ContractQueue = .{},

    threadlocal var current: ?*Thread = null;

    fn worker(thread_ripper: *ThreadRipper) !void {
        var thread = Thread{};
        current = &thread;

        // add the thread to the threads linkedlist
        thread_ripper.register(&thread);
        defer thread_ripper.unregister(&thread);

        // set is_waking
        var is_waking = false;
        while (true) {
            is_waking = thread_ripper.wait(is_waking) catch return;
            while (thread.pop(thread_ripper)) |stolen_node| {
                if (stolen_node.pushed or is_waking) {
                    thread_ripper.notify(is_waking);
                    // var workingCount = thread_ripper.workingCount.load(.monotonic);
                    // while (true) {
                    //     if (workingCount < thread_ripper.max_threads) {
                    //         print("\nIncrementing worker count: {d}\n", .{workingCount});
                    //         workingCount = thread_ripper.workingCount.cmpxchgStrong(workingCount, workingCount + 1, .acquire, .monotonic) orelse break;
                    //     }
                    // }
                }
                const run_node = stolen_node.node;
                const runFn = run_node.data.runFn;
                is_waking = false;
                try runFn(&run_node.data);
            } else {
                // var workingCount = thread_ripper.workingCount.load(.monotonic);
                // while (true) {
                //     if (workingCount > 0) {
                //         print("\nDecrementing worker count: {d}\n", .{workingCount});
                //         workingCount = thread_ripper.workingCount.cmpxchgStrong(workingCount, workingCount - 1, .acquire, .monotonic) orelse break;
                //     }
                // }
            }
        }
    }

    /// Try to dequeue a Node/Job from the ThreadRipper.
    /// Spurious reports of dequeue() returning empty are allowed.
    fn pop(noalias thread: *Thread, noalias thread_ripper: *ThreadRipper) ?JobQueue.Stole {
        // Check our local buffer first
        if (thread.job_queue.dequeue()) |node| {
            return JobQueue.Stole{
                .node = node,
                .pushed = false,
            };
        }

        // Then check our local queue
        if (thread.job_queue.consume(&thread.contract_queue)) |stole| {
            return stole;
        }

        // Then the global queue
        if (thread.job_queue.consume(&thread_ripper.global_queue)) |stole| {
            return stole;
        }

        // TODO: add optimistic I/O polling here

        // Then try work stealing from other threads
        // print("Nothing to consume\n", .{});
        const sync: Sync = @bitCast(thread_ripper.sync.load(.monotonic));
        var num_threads: u32 = sync.spawned_threads;
        // print("number of threads {d}\n", .{num_threads});
        while (num_threads > 0) : (num_threads -= 1) {
            // Traverse the stack of registered threads on the thread pool
            const target = thread.target orelse thread_ripper.threads_stack.load(.acquire) orelse unreachable;
            // if (target == null) return null;
            thread.target = target.next;

            // Try to steal from their queue first to avoid contention (the target steal's from queue last).
            if (thread.job_queue.consume(&target.contract_queue)) |stole| {
                return stole;
            }

            // Skip stealing from the buffer if we're the target.
            // We still steal from our own queue above given it may have just been locked the first time we tried.
            if (target == thread) {
                continue;
            }

            // Steal from the buffer of a remote thread as a last resort
            if (thread.job_queue.steal(&target.job_queue)) |stole| {
                return stole;
            }
        }

        return null;
    }
};

/// An event which stores 1 semaphore token and is multi-threaded safe.
/// The event can be shutdown(), waking up all wait()ing threads and
/// making subsequent wait()'s return immediately.
const Event = struct {
    const EMPTY = 0;
    const WAITING = 1;
    const NOTIFIED = 2;
    const SHUTDOWN = 3;

    state: Atomic(u32) = Atomic(u32).init(EMPTY),
    /// Wait for and consume a notification
    /// or wait for the event to be shutdown entirely
    noinline fn wait(event: *Event) void {
        var acquire_with: u32 = EMPTY;
        var state = event.state.load(.monotonic);
        // print("Event state {d}", .{state});

        while (true) {
            // If we're shutdown then exit early.
            // Acquire barrier to ensure operations before the shutdown() are seen after the wait().
            // Shutdown is rare so it's better to have an Acquire barrier here instead of on CAS failure + load which are common.
            if (state == SHUTDOWN) {
                // std.atomic.Value(u32).fence(.acquire);
                return;
            }

            // Consume a notification when it pops up.
            // Acquire barrier to ensure operations before the notify() appear after the wait().
            // if we are a notified event we return else continue the loop
            if (state == NOTIFIED) {
                state = event.state.cmpxchgStrong(
                    state,
                    acquire_with,
                    .acquire,
                    .monotonic,
                ) orelse return;
                continue;
            }

            // There is no notification to consume, we should wait on the event by ensuring its WAITING.
            // if we are not a waiting state then we set the state to waiting if this succeeds we break the loop
            // and loop again;
            if (state != WAITING) blk: {
                state = event.state.cmpxchgStrong(
                    state,
                    WAITING,
                    .monotonic,
                    .monotonic,
                ) orelse break :blk;
                continue;
            }

            // Wait on the event until a notify() or shutdown().
            // If we wake up to a notification, we must acquire it with WAITING instead of EMPTY
            // since there may be other threads sleeping on the Futex who haven't been woken up yet.
            //
            // Acquiring to WAITING will make the next notify() or shutdown() wake a sleeping futex thread
            // who will either exit on SHUTDOWN or acquire with WAITING again, ensuring all threads are awoken.
            // This unfortunately results in the last notify() or shutdown() doing an extra futex wake but that's fine.
            // print("We spin up the Futex...\n", .{});
            // print("Wake threads {}\n", .{&event.state});
            std.Thread.Futex.wait(&event.state, WAITING);
            state = event.state.load(.monotonic);
            acquire_with = WAITING;
            // print("We made it passed the futex...\n", .{});
        }
    }

    /// Post a notification to the event if it doesn't have one already
    /// then wake up a waiting thread if there is one as well.
    fn notify(event: *Event) void {
        return event.wake(NOTIFIED, 1);
    }

    /// Marks the event as shutdown, making all future wait()'s return immediately.
    /// Then wakes up any threads currently waiting on the Event.
    fn shutdown(event: *Event) void {
        return event.wake(SHUTDOWN, std.math.maxInt(u32));
    }

    fn wake(event: *Event, release_with: u32, wake_threads: u32) void {
        // Update the Event to notifty it with the new `release_with` state (either NOTIFIED or SHUTDOWN).
        // release barrier to ensure any operations before this are this to happen before the wait() in the other threads.
        const state = event.state.swap(release_with, .release);

        // Only wake threads sleeping in futex if the state is WAITING.
        // Avoids unnecessary wake ups.
        if (state == WAITING) {
            // print("Wake threads {}\n", .{&event.state});
            std.Thread.Futex.wake(&event.state, wake_threads);
        }
    }
};

// FIFO
// We add to front and take from the back
const JobQueue = struct {
    // Head is atomic since when we add jobs nodes to the queue or consume job nodes, we dont want other
    // threads to be grabbing our shit.
    const capacity: u32 = 50;
    head: Atomic(u32) = Atomic(u32).init(0),
    tail: Atomic(u32) = Atomic(u32).init(0),
    buffer: [capacity]Atomic(*Node) = undefined,
    pub fn enqueue(job_queue: *JobQueue, job_list: *JobList) error{Overflow}!void {
        // We do not need to have this execute in the correct order of writes or reads
        var head = job_queue.head.load(.monotonic);
        var tail = job_queue.tail.load(.monotonic); // We are the only one who can change this

        while (true) {
            var size = tail -% head;
            assert(size <= capacity);
            // Attempt to fill the buffer;
            if (size < capacity) {
                // We set the current_node to the list head
                var current_node: ?*Node = job_list.head;
                while (size < capacity) : (size += 1) {
                    const node_to_add = current_node orelse break;
                    // print("Attempting to add node to buffer\n", .{});
                    // we set the current_node to the node next in the job_list;
                    current_node = current_node.?.next;
                    // tail = 30 then capacity equals 256 then tail % capacity = 30
                    job_queue.buffer[tail % capacity].store(node_to_add, .unordered);
                    tail +%= 1;
                }
                // release barrier synchronizes with Acquire loads for steal()ers to see the array writes.
                job_queue.tail.store(tail, .release);

                // Update the list so that all the nodes which were added are removed from the argument list
                job_list.head = current_node orelse return;
                std.atomic.spinLoopHint();
                // try again if there's more. we try again since the head and tail of the buffer can change
                // the job_queue can also change, since threads can steal from this queue
                head = job_queue.head.load(.monotonic);
                continue;
            }

            // Here we assume the buffer is overflowed so we can't add any items
            // so instead we first try to see if any items has been taken from the buffer
            // if we can't take the head again another thread is grabbing it
            // thus we take the current buffer and create a linkedlist of the first half of the buffer
            // and add the passed in job_list and add it to the end;
            // Then we return overflowed since we couldnt grab the head since another thread is taking
            // and we couldnt add items which means it overflowed for now
            // remeber the buffer size stays at 256 always it the nodes inside that change

            // Try to steal half of the tasks in the buffer to make room for future push()es.
            // Migrating half amortizes the cost of stealing while requiring
            // future pops to still use the buffer.
            // Acquire barrier to ensure the linked list creation after the steal only happens
            // after we succesfully steal.

            // The migration_size ie the size of the current buffer / 2
            var migration_size = size / 2;
            // Here we increment the head node by half essentially we try to steal half the buffer
            // remember the job_queue head is the buffer index so if we increment it by double
            // here we try to aquire the head of the job_queue and replace it with double the size so that
            // we can add more items into the buffer
            head = job_queue.head.cmpxchgStrong(
                head,
                head +% migration_size,
                .acquire,
                .monotonic,
            ) orelse {
                // This occures when we succeed to grab the head;
                // Link the migrated Nodes together
                // here we grab the first half of the buffer
                const first: *Node = job_queue.buffer[head % capacity].load(.monotonic);
                // then while the migration_size is greater than zero
                while (migration_size > 0) : (migration_size -= 1) {
                    // We grab the current_node from the begining of the buffer and link it to the next one
                    // We create a linked list from the job_queue.buffer
                    const prev: *Node = job_queue.buffer[head % capacity].load(.monotonic);
                    // print("Prev Grabbing the first node increment head \n", .{});
                    head +%= 1;
                    prev.next = job_queue.buffer[head % capacity].load(.monotonic);
                }
                // at this point we have a linked list the starts from the first to the end

                // Append the list that was supposed to be pushed to the end of the migrated Nodes
                // Here we grab the last node in the job_queue.buffer, remember we are not altering the buffer

                const last = job_queue.buffer[(head -% 1) % capacity].load(.monotonic);
                // now we take the list that was passed in and add it to the back of the linkedlist we created
                last.next = job_list.head;
                job_list.tail.next = null;

                // Return the migrated nodes + the original job_list as overflowed
                // we then set the job_list to the new linkedlist and return overflow as the buffer is overflowed
                job_list.head = first;
                return error.Overflow;
            };
        }
    }
    pub fn dequeue(job_queue: *JobQueue) ?*Node {
        // Here we grab the buffer index start and end of the buffer
        var head = job_queue.head.load(.monotonic);
        const tail = job_queue.tail.load(.monotonic); // we're the only thread that can change this

        // We then loop and try and grab the head of the buffer and exchange it for head+%1
        // If we can't do this we return the buffer node
        while (true) {
            // Quick sanity check and return null when queue is empty
            const size = tail -% head;
            assert(size <= capacity);
            if (size == 0) {
                return null;
            }

            // Dequeue with an acquire barrier to ensure any writes done to the Node
            // only happen after we succesfully claim it from the array.
            // We do the inverse for performance eseentially if a thread trys to exchange then they shoud
            // loop again this way they the own they keep poping
            // if a thread fails it atomically reads the buffer and returns it
            // this way we don't constantly retry returns the popped value instead the failing ones do
            // also we load atomically so then we load from the buffer which can only happen one at a time
            // the question is though can't two threads load the same value, because if two fail
            // then two load the same value
            // print("Poping the buffer {d} \n", .{job_queue.buffer[head % capacity].load(.monotonic).id});
            head = job_queue.head.cmpxchgStrong(
                head,
                head +% 1,
                .acquire,
                .monotonic,
            ) orelse return job_queue.buffer[head % capacity].load(.monotonic);
        }
    }

    const Stole = struct {
        node: *Node,
        pushed: bool,
    };

    // This function consumes nodes from the queue and pushes them to the buffer
    // This is for the thread to use to consume from queues and push to there queue
    fn consume(job_queue: *JobQueue, contract_queue: *ContractQueue) ?Stole {
        // We first attempt to get a consumer
        var consumer = contract_queue.tryAcquireConsumer() catch return null;
        defer contract_queue.releaseConsumer(consumer);

        const head = job_queue.head.load(.monotonic);
        const tail = job_queue.tail.load(.monotonic); // we're the only thread that can change this

        const size = tail -% head;
        assert(size <= capacity);
        assert(size == 0); // we should only be consuming if our array is empty

        // Pop nodes from the queue and push them to our array.
        // Atomic stores to the array as steal() threads may be atomically reading from it.
        var pushed: u32 = 0;
        while (pushed < capacity) : (pushed += 1) {
            const node = contract_queue.pop(&consumer) orelse break;
            // print("Adding the nodes to the buffer {d} \n", .{node.id});
            // in pop we set the consumer to the next node
            // 30 +% 0 = 30 => 30 % capacity = 30;
            job_queue.buffer[(tail +% pushed) % capacity].store(node, .unordered);
        }

        // We will be returning one node that we stole from the queue.
        // Get an extra, and if that's not possible, take one from our array.
        // We attempt to grab another node if its null and we didnt push anything we return null
        // else we load the last node value in the buffer and break
        // this way we either grab the next node in the contract queue or return the last buffer node value
        // print("Grab the next node in the contract_queue or buffer\n", .{});
        const node = contract_queue.pop(&consumer) orelse blk: {
            if (pushed == 0) return null;
            pushed -= 1;
            break :blk job_queue.buffer[(tail +% pushed) % capacity].load(.monotonic);
        };
        // print("Stolen node {d}\n", .{node.id});

        // Update the array tail with the nodes we pushed to it.
        // release barrier to synchronize with Acquire barrier in steal()'s to see the written array Nodes.
        // if we pushed then we store the new tail value since its been updated
        if (pushed > 0) job_queue.tail.store(tail +% pushed, .release);
        // then we return the stolen node
        return Stole{
            .node = node,
            .pushed = pushed > 0,
        };
    }

    // steal steals from other threads job_queues and sotre them in their job_queue
    fn steal(noalias job_queue: *JobQueue, noalias work_stealing_queue: *JobQueue) ?Stole {
        const head = job_queue.head.load(.monotonic);
        const tail = job_queue.tail.load(.monotonic); // we're the only thread that can change this

        const size = tail -% head;
        assert(size <= capacity);
        assert(size == 0); // we should only be stealing if our array is empty

        while (true) : (std.atomic.spinLoopHint()) {
            const stealable_buffer_head = work_stealing_queue.head.load(.acquire);
            const stealable_buffer_tail = work_stealing_queue.tail.load(.acquire);

            // Overly large size indicates the the tail was updated a lot after the head was loaded.
            // Reload both and try again.
            // we grab the size
            const stealable_buffer_size = stealable_buffer_tail -% stealable_buffer_head;
            if (stealable_buffer_size > capacity) {
                continue;
            }

            // Try to steal half (divCeil) to amortize the cost of stealing from other threads.
            const steal_size = stealable_buffer_size - (stealable_buffer_size / 2);
            if (steal_size == 0) {
                return null;
            }

            // Copy the nodes we will steal from the target's array to our own.
            // Atomically load from the target buffer array as it may be pushing and atomically storing to it.
            // Atomic store to our array as other steal() threads may be atomically loading from it as above.
            var i: usize = 0;
            while (i < steal_size) : (i += 1) {
                // here we grab the stolen_job_queue node
                const node = work_stealing_queue.buffer[(stealable_buffer_head +% i) % capacity].load(.unordered);
                // then we store it in the job_queue
                job_queue.buffer[(tail +% i) % capacity].store(node, .unordered);
            }

            // Try to commit the steal from the target buffer using:
            // - an Acquire barrier to ensure that we only interact with the stolen Nodes after the steal was committed.
            // - a release barrier to ensure that the Nodes are copied above prior to the committing of the steal
            //   because if they're copied after the steal, the could be getting rewritten by the target's push().
            _ = work_stealing_queue.head.cmpxchgStrong(
                stealable_buffer_head,
                stealable_buffer_head +% steal_size,
                .acq_rel,
                .monotonic,
            ) orelse {
                // Pop one from the nodes we stole as we'll be returning it

                const pushed = steal_size - 1;
                const node = job_queue.buffer[(tail +% pushed) % capacity].load(.monotonic);

                // Update the array tail with the nodes we pushed to it.
                // release barrier to synchronize with Acquire barrier in steal()'s to see the written array Nodes.
                if (pushed > 0) job_queue.tail.store(tail +% pushed, .release);
                return Stole{
                    .node = node,
                    .pushed = pushed > 0,
                };
            };
        }
    }
};

// LIFO Stack
const ContractQueue = struct {
    // the stack holds ptrs to the nodes
    stack: Atomic(usize) = Atomic(usize).init(0),
    cache: ?*Node = null,

    // a Bit flag added to a stack address to determine if it is beign consumed or has a cache
    const HAS_CACHE: usize = 0b01;
    const IS_CONSUMING: usize = 0b10;
    // PTRMASK is used to essentailly mask or tape of the last two bits so we get just the node to the stack
    // ~ is the inverse of the 0b01 and 0b10 so its 0b111111111111100
    const PTR_MASK: usize = ~(HAS_CACHE | IS_CONSUMING); // ~(0b01 | 0b10) = ~(0b11) = 0b...11111111111100

    // & checks that both bits are the same if both are 1 then it returns 1
    // | cehcks that bit is either 1 or 0 and resturns 1 if 1 of the bits is 1
    // ~ is the inverse so 0000 => ~0000 = 1111
    comptime {
        assert(@alignOf(Node) >= ((IS_CONSUMING | HAS_CACHE) + 1));
    }

    fn push(contract_queue: *ContractQueue, job_list: *JobList) void {
        // print("Pushing to the contract_queue...\n", .{});
        // Here we take the contract_queue and job_list and try to add it to the stack
        // we atomically load the stack from mem
        var stack = contract_queue.stack.load(.monotonic);
        // print("Loading the stack {d} \n", .{stack});
        // print("----------Job_list{any}\n", .{job_list});
        while (true) {
            // we add the stack to the end of the job_list
            // var pointer_value = stack & PTR_MASK, masks out the last to bits ;
            // here we take the stack 0b10111.... and the PTR_MASK 0b1111111...00
            // thus the last two bits are masked out sicne the PTR_MASK is always 00
            job_list.tail.next = @ptrFromInt(stack & PTR_MASK);
            // print("Adding stack to the tail\n", .{});

            // Update the stack with the list (pt. 2).
            // Don't change the HAS_CACHE and IS_CONSUMING bits of the consumer.
            // Here we create a new stack with the job_list.head as the start
            // basically just converts the value to a usize its just the node of the node

            var new_stack = @intFromPtr(job_list.head);
            // print("creating new stack\n", .{});
            // we assert that the new_stack masked with the inverse == 0
            // PTR_MASK = 0b0000000..11 and so the result is 0b0000000000 because the job_list head should have 00 at the last two bits
            assert(new_stack & ~PTR_MASK == 0);
            // then we take the current stack & ~PTR_MASK which as seen above extracts the bit flags
            // we then set the bit flags to the new_stack this way when we compare and exchange another thread
            // knows if the newstack has a chached value or is being consumed
            // here we take the new_stack and add the bit flags to it so the new stack contains the bitflags
            new_stack |= (stack & ~PTR_MASK);
            // print("New stack ptr {d} \n", .{new_stack});
            // print("Loading the stack {d} \n", .{stack});

            // Push to the stack with a release barrier for the consumer to see the proper list links.
            // So if we succeed with the stack excahnge we then break
            stack = contract_queue.stack.cmpxchgStrong(
                stack,
                new_stack,
                .release,
                .monotonic,
            ) orelse break;
        }
    }

    // The point here is to grab the consumer atomic-lock
    // We first load the stack
    // then we check if the bit flag is_consuming is set to 1 if so we return
    // same thign for the cache andnode
    // if the bit flag for the cache is 0 no cache node then we
    fn tryAcquireConsumer(contract_queue: *ContractQueue) error{ Empty, Contended }!?*Node {
        var stack = contract_queue.stack.load(.monotonic);
        while (true) {
            // Here we check the bit_flag is consuming is set to 0 else it has a consumer
            // Here we comapre is consuming with is 0b10 with the stack the & check both ....10 are the same
            // stack & IS_CONSUMING => 0000000000..10=2 or 000000000..00=0
            if (stack & IS_CONSUMING != 0)
                return error.Contended; // The queue already has a consumer.
            // here we check if the ptr_value of the stack is zero ie has not been set and there is empty
            // we also check if anything is chached
            if (stack & (HAS_CACHE | PTR_MASK) == 0)
                return error.Empty; // The queue is empty when there's nothing cached and nothing in the stack.

            // When we acquire the consumer, also consume the pushed stack if the cache is empty.
            // here we set the new stack to the stack with the bitflags
            var new_stack = stack | HAS_CACHE | IS_CONSUMING;
            // so now the new stack is the old stack with the old cache value which could be zero and the
            // is_consuming flag
            // If the stack cache value is 0
            if (stack & HAS_CACHE == 0) {
                // then we assert that the stack is not 0 because if the cache is zero and the stack is 0
                // then its empty
                // otherwise we reset the stack to 0 since ~PTR_MASK is 0000000000..,..
                assert(stack & PTR_MASK != 0);
                // so now the stack is 0 and contains the correct bitflags
                new_stack &= ~PTR_MASK;
                // 000000000000..10 basically state that the stack is being consumed
            }
            // then we cas if we still own the old stack we excahnge it with 00000000.10 so we now are the consumer
            // if this succceds then we know that we can consume more
            // other threads will fail the cas and return the cache value if there is one or
            // the ptr_stack with the bitflags masked off since if there isnt a cache or isoncomuing then just return the node

            // Acquire barrier on getting the consumer to see cache/Node updates done by previous consumers
            // and to ensure our cache/Node updates in pop() happen after that of previous consumers.
            stack = contract_queue.stack.cmpxchgStrong(
                stack,
                new_stack,
                .acquire,
                .monotonic,
                // If we succeed with the cas then we return the stack or cached value
            ) orelse return contract_queue.cache orelse @ptrFromInt(stack & PTR_MASK);
        }
    }

    fn releaseConsumer(noalias contract_queue: *ContractQueue, noalias consumer: ?*Node) void {
        // Stop consuming and remove the HAS_CACHE bit as well if the consumer's cache is empty.
        // When HAS_CACHE bit is zeroed, the next consumer will acquire the pushed stack nodes.
        var remove = IS_CONSUMING;
        // If the consumer is null then this means the has_cache is null so we add it to the remove
        if (consumer == null)
            remove |= HAS_CACHE;

        // release the consumer with a release barrier to ensure cache/node accesses
        // happen before the consumer was released and before the next consumer starts using the cache.
        // if we have popped then the consumer is now the next node so now the cache holds the next node
        contract_queue.cache = consumer;
        // we remove the is_cosnuming and the has_cache if consumer is null
        // we remove the current is_consumong and cache vlaue if the next node value null;
        const stack = contract_queue.stack.fetchSub(remove, .release);
        // we assert then the old stack & remove is != 0 since the old_stack had is consuming flag then this will be true
        assert(stack & remove != 0);
    }

    fn pop(noalias contract_queue: *ContractQueue, noalias consumer_ref: *?*Node) ?*Node {
        // Check the consumer cache (fast path)
        if (consumer_ref.*) |node| {
            consumer_ref.* = node.next;
            return node;
        }

        // Load the stack to see if there was anything pushed that we could grab.
        var stack = contract_queue.stack.load(.monotonic);
        assert(stack & IS_CONSUMING != 0);
        if (stack & PTR_MASK == 0) {
            return null;
        }

        // Nodes have been pushed to the stack, grab then with an Acquire barrier to see the Node links.
        // this swaps out the current stack node and replaces it with 000000..11;
        // this shows the other threads we are consuming
        stack = contract_queue.stack.swap(HAS_CACHE | IS_CONSUMING, .acquire);
        assert(stack & IS_CONSUMING != 0);
        assert(stack & PTR_MASK != 0);

        // now that we have the list of nodes we
        // this is the current node in the stack
        const node: *Node = @ptrFromInt(stack & PTR_MASK);
        // we grab the node and set the consumer_ref to the next node in the node list
        // we pop it off and retrun the nodenode
        consumer_ref.* = node.next;
        return node;
    }
};

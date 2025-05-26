const std = @import("std");
const ThreadRipper = @import("ThreadRipper.zig");
const GK = ThreadRipper.GateKeeper;
const print = std.debug.print;
const Atomic = std.atomic.Value;
var counter: Atomic(u32) = Atomic(u32).init(0);
const WaitGroup = std.Thread.WaitGroup;

var start: i128 = 0;
var count: usize = 0;
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    const allocator = gpa.allocator();
    // var thread_ripper: ThreadRipper = undefined;
    // const options = ThreadRipper.Options{
    //     .max_threads = @intCast(std.Thread.getCpuCount() catch 6),
    //     .arena = &allocator,
    // };
    // try thread_ripper.init(options);
    // thread_ripper.warm();
    // var gk: GK = undefined;
    // try gk.init(thread_ripper.arena);
    // gk.action_list.* = try generateLotsOfJobs(&thread_ripper);
    // defer gk.deinit();
    // const ids = [_]usize{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    // for (0..10) |i| {
    //     try thread_ripper.fork(&gk, testThreadFuncATOMIC, .{i + 1});
    // }
    // start = std.time.milliTimestamp(); // Use the standard system clock
    // try testThreadFuncATOMIC(0);
    // try thread_ripper.waitAndWork(&gk);

    // defer thread_ripper.deinit();

    // print("Disptach batch \n", .{});
    // while (true) {}
    // while (thread_ripper.working()) {}

    start = std.time.milliTimestamp(); // Use the standard system clock

    // var pool: std.Thread.Pool = undefined;
    // try std.Thread.Pool.init(&pool, .{ .n_jobs = @intCast(std.Thread.getCpuCount() catch 6), .allocator = allocator });
    // defer pool.deinit();
    // //
    // var wait_group: WaitGroup = undefined;
    // wait_group.reset();
    // // try testTwoJobs(&thread_ripper, &wait_group);
    // // while (true) {}
    var queues: []*std.ArrayList(?usize) = try allocator.alloc(*std.ArrayList(?usize), 10);
    for (0..std.Thread.getCpuCount() catch 6) |i| {
        var queue = std.ArrayList(?usize).init(allocator);
        queues[i] = &queue;
        // var thread = try std.Thread.spawn(.{}, testThreadFuncMutex, .{queue});
        // thread.detach();
    }

    var counter_: usize = 0;
    while (counter_ < 1000000 / 10) : (counter_ += 1) {
        try queues[counter_ % 10].append(1);
        counter_ += 1;
    }

    // while (count < 99999999) : (counter_ += 1) {
    //     var node = std.SinglyLinkedList(usize).Node{
    //         .data = 1,
    //     };
    //     queues[counter_ % 10].prepend(&node);
    // }
    //
    for (queues) |q| {
        var thread = try std.Thread.spawn(.{}, testThreadFuncMutex, .{q});
        thread.detach();
    }

    // pool.waitAndWork(&wait_group);
    while (true) {}
}

fn testThreadFuncMutex(queue: *std.ArrayList(?usize)) void {
    // wait_group.start();
    // defer wait_group.finish();

    while (queue.pop()) |inc| {
        if (count >= 1000000) {
            const end = std.time.milliTimestamp();
            const duration = end - start;
            std.debug.print("Function ran for {any} miliseconds\n", .{duration});
            break;
        }
        count += inc;
        print("{any}\n", .{count});
        // Remove the 'orelse break' so the thread keeps trying if it fails
    }
}

fn testThreadFuncATOMIC(id: usize) !void {
    while (true) {
        if (count == 99999999) {
            const end = std.time.milliTimestamp();
            const duration = end - start;
            std.debug.print("Function ran for {any} miliseconds\n", .{duration});
            print("Id: {any}, count: {any}\n", .{ id, count });
            break;
        }
        count += 1;
        // Remove the 'orelse break' so the thread keeps trying if it fails
    }
    return;
}

fn increment() !void {
    count += 1;
    if (count >= 999999) {
        const end = std.time.milliTimestamp();
        const duration = end - start;
        std.debug.print("Function ran for {d} nanoseconds\n", .{duration / 9});
    }
}

fn generateLotsOfJobs(thread_ripper: *ThreadRipper) !ThreadRipper.ActionList {
    var job_list = ThreadRipper.ActionList{
        .head = undefined,
        .tail = undefined,
    };

    var node = try thread_ripper.generateJob(testThreadFuncATOMIC, .{0});
    node.id = 1000000;
    node.id = @intCast(std.Thread.getCpuCount() catch 6);
    const tail: *ThreadRipper.Node = node;
    var head: *ThreadRipper.Node = undefined;
    for (0..100) |i| {
        head = try thread_ripper.generateJob(testThreadFuncATOMIC, .{0});
        head.id = @intCast(i);
        head.next = node;
        node = head;
    }
    job_list.head = head;
    job_list.tail = tail;

    return job_list;
}

fn incrementBatch(start_size: usize, end_size: usize, wait_group: *WaitGroup) !void {
    wait_group.start();
    defer wait_group.finish();
    for (start_size..end_size) |_| {
        const count_v = counter.fetchAdd(1, .seq_cst);
        if (count_v == 999999) {
            const end = std.time.milliTimestamp();
            const duration = end - start;
            std.debug.print("Function ran for {any} miliseconds\n", .{duration});
        }
    }
}

fn generateBatchedJobs(thread_ripper: *ThreadRipper, wait_group: *WaitGroup) !ThreadRipper.JobList {
    var job_list = ThreadRipper.JobList{
        .head = undefined,
        .tail = undefined,
    };

    const batch_size = 10000;
    const job_count = 1000000 / batch_size;
    print("\nJob Count: {d}\n", .{job_count});

    var node = try thread_ripper.generateJob(
        incrementBatch,
        .{ job_count * batch_size, (job_count + 1) * batch_size, wait_group },
    );

    node.id = 1000000;

    const tail: *ThreadRipper.Node = node;
    var head: *ThreadRipper.Node = undefined;

    for (0..job_count) |i| {
        head = try thread_ripper.generateJob(
            incrementBatch,
            .{ i * batch_size, (i + 1) * batch_size, wait_group },
        );
        head.id = @intCast(i);
        head.next = node;
        node = head;
    }

    job_list.head = head;
    job_list.tail = tail;

    return job_list;
}

fn testTwoJobs(thread_ripper: *ThreadRipper) !void {
    var job_list = try generateLotsOfJobs(thread_ripper);
    print("Running jobs\n", .{});
    start = std.time.milliTimestamp(); // Use the standard system clock
    thread_ripper.dispatchBatch(&job_list);
}

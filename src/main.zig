const std = @import("std");
const ThreadRipper = @import("ThreadRipper.zig");
const print = std.debug.print;
const Atomic = std.atomic.Value;
var counter: Atomic(u32) = Atomic(u32).init(0);
const WaitGroup = std.Thread.WaitGroup;

var start: i128 = 0;
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var allocator = gpa.allocator();
    var thread_ripper: ThreadRipper = undefined;
    const options = ThreadRipper.Options{
        .max_threads = @intCast(std.Thread.getCpuCount() catch 6),
        .arena = &allocator,
    };
    try thread_ripper.init(options);
    // defer thread_ripper.deinit();

    // print("Disptach batch \n", .{});
    try testTwoJobs(&thread_ripper);
    while (true) {}
    // while (thread_ripper.working()) {}

    // var pool: std.Thread.Pool = undefined;
    // try std.Thread.Pool.init(&pool, .{ .n_jobs = @intCast(std.Thread.getCpuCount() catch 6), .allocator = allocator });
    // defer pool.deinit();
    //
    // var wait_group: WaitGroup = undefined;
    // wait_group.reset();
    // start = std.time.nanoTimestamp(); // Use the standard system clock
    // for (0..std.Thread.getCpuCount() catch 6) |_| {
    //     try pool.spawn(testThreadFunc, .{&wait_group});
    // }
    //
    // pool.waitAndWork(&wait_group);
}

var count: usize = 0;

fn testThreadFunc(wait_group: *WaitGroup) void {
    wait_group.start();
    defer wait_group.finish();

    while (true) {
        if (count >= 999999) {
            const end = std.time.nanoTimestamp();
            const duration = end - start;
            std.debug.print("Function ran for {d} nanoseconds\n", .{duration});
            break;
        }
        count += 1;
        // Remove the 'orelse break' so the thread keeps trying if it fails
    }
}

fn testThreadFuncATOMIC(_: *ThreadRipper) !void {
    // if (!thread_ripper.working()) return;
    // std.time.sleep(1_000_000_000);
    while (true) {
        if (count >= 999999) {
            const end = std.time.nanoTimestamp();
            const duration = end - start;
            std.debug.print("Function ran for {d} nanoseconds\n", .{duration});
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
        const end = std.time.nanoTimestamp();
        const duration = end - start;
        std.debug.print("Function ran for {d} nanoseconds\n", .{duration});
    }
}

fn generateLotsOfJobs(thread_ripper: *ThreadRipper) !ThreadRipper.JobList {
    var job_list = ThreadRipper.JobList{
        .head = undefined,
        .tail = undefined,
    };

    var node = try thread_ripper.generateJob(increment, .{});
    node.id = 1000000;
    // node.id = @intCast(std.Thread.getCpuCount() catch 6);
    const tail: *ThreadRipper.Node = node;
    var head: *ThreadRipper.Node = undefined;
    for (0..1000000) |i| {
        head = try thread_ripper.generateJob(increment, .{});
        head.id = @intCast(i);
        head.next = node;
        node = head;
    }
    job_list.head = head;
    job_list.tail = tail;

    return job_list;
}

fn incrementBatch(start_size: usize, end_size: usize) !void {
    for (start_size..end_size) |_| {
        const count_v = counter.fetchAdd(1, .seq_cst);
        if (count_v == 999999) {
            const end = std.time.nanoTimestamp();
            const duration = end - start;
            std.debug.print("Function ran for {d} nanoseconds\n", .{duration});
        }
    }
}

fn generateBatchedJobs(thread_ripper: *ThreadRipper) !ThreadRipper.JobList {
    var job_list = ThreadRipper.JobList{
        .head = undefined,
        .tail = undefined,
    };

    const batch_size = 10000;
    const job_count = 1000000 / batch_size;
    print("\nJob Count: {d}\n", .{job_count});

    var node = try thread_ripper.generateJob(
        incrementBatch,
        .{ job_count * batch_size, (job_count + 1) * batch_size },
    );

    node.id = 1000000;

    const tail: *ThreadRipper.Node = node;
    var head: *ThreadRipper.Node = undefined;

    for (0..job_count) |i| {
        head = try thread_ripper.generateJob(
            incrementBatch,
            .{ i * batch_size, (i + 1) * batch_size },
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
    var job_list = try generateBatchedJobs(thread_ripper);
    start = std.time.nanoTimestamp(); // Use the standard system clock
    print("Running jobs\n", .{});
    thread_ripper.dispatchBatch(&job_list);
}

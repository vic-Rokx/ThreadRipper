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
        .max_threads = 6,
        .arena = &allocator,
    };
    try thread_ripper.init(options);

    // print("Disptach batch \n", .{});
    try testTwoJobs(&thread_ripper);
    while (true) {}

    // while (thread_ripper.done()) {}

    // var pool: std.Thread.Pool = undefined;
    // try std.Thread.Pool.init(&pool, .{ .n_jobs = 6, .allocator = allocator });
    // defer pool.deinit();
    //
    //
    // var wait_group: WaitGroup = undefined;
    // wait_group.reset();
    //
    // start = std.time.nanoTimestamp(); // Use the standard system clock
    // for (0..100) |_| {
    //     try pool.spawn(testThreadFunc, .{&wait_group});
    // }
    //
    // pool.waitAndWork(&wait_group);
}

fn testThreadFunc(wait_group: *WaitGroup) void {
    wait_group.start();
    defer wait_group.finish();
    // std.time.sleep(1_000_000_000);
    var current_counter = counter.load(.monotonic);
    while (true) {
        if (current_counter == 9999) {
            const end = std.time.nanoTimestamp(); // Use the standard system clock
            const duration = end - start; // Calculate the duration
            std.debug.print("Function ran for {d} nanoseconds\n", .{duration});
        }
        current_counter = counter.cmpxchgStrong(current_counter, current_counter + 1, .acquire, .monotonic) orelse break;
    }
}

fn testThreadFuncATOMIC() !void {
    // std.time.sleep(1_000_000_000);
    var current_counter = counter.load(.monotonic);
    while (true) {
        if (current_counter == 9999) {
            const end = std.time.nanoTimestamp(); // Use the standard system clock
            const duration = end - start; // Calculate the duration
            std.debug.print("Function ran for {d} nanoseconds\n", .{duration});
        }
        current_counter = counter.cmpxchgStrong(current_counter, current_counter + 1, .acquire, .monotonic) orelse break;
    }
}

fn generateLotsOfJobs(thread_ripper: *ThreadRipper) !ThreadRipper.JobList {
    var job_list = ThreadRipper.JobList{
        .head = undefined,
        .tail = undefined,
    };

    var node = try thread_ripper.generateJob(testThreadFuncATOMIC, .{});
    node.id = 10000;
    const tail: *ThreadRipper.Node = node;
    var head: *ThreadRipper.Node = undefined;
    for (0..10000) |i| {
        head = try thread_ripper.generateJob(testThreadFuncATOMIC, .{});
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
    // const node1 = try thread_ripper.generateJob(testThreadFunc, .{});
    // node1.id = 1;
    // const node2 = try thread_ripper.generateJob(testThreadFunc, .{});
    // node2.id = 2;
    // node1.next = node2;

    // var job_list = ThreadRipper.JobList{
    //     .head = node1,
    //     .tail = node2,
    // };

    start = std.time.nanoTimestamp(); // Use the standard system clock
    thread_ripper.dispatchBatch(&job_list);
}

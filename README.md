Threadly
========

A library of java tools to assist with concurrent development. There are tools to help with a wide range of development and testing. This is designed to be a complement to java.util.concurrent and uses java.util.concurrent to help assist in it's implementations where it makes sense. This library is particularly useful for getting legacy concurrent code and getting it under test.

-- Notable Unit Test Tools --

*    TestablePriorityScheduler - Probably the largest gem in the tool kit. This scheduler is designed to take blocking and concurrent code and make it more controllable. It only allows one thread to execute at any given time. In addition threads are progressed forward by .tick() calls to the scheduler.

It relies on any code that performs blocking uses an injectable LockFactory, then when under test TestablePriorityScheduler can be provided as the lock factory. When actions like .wait .sleep occur on locks that the testable scheduler produced it will yield those threads and give control to other threads.

*    TestCondition - often times in doing unit test for asynchronous operations you have to wait for a condition to be come true. This class gives a way to easily wait for those conditions to be true, or throw an exception if they do not happen after a given timeout. The implementation of TestRunnable gives a good example of how this can be used.

*    TestRunnable - a runnable structure that has common operations already implemented. It gives two functions handleRunStart and handleRunFinish to allow people to optionally override to provide any test specific operation which is necessary. You can see many examples in our own unit test code of how we are using this.

-- Notable Concurrency Tools --

*    PriorityScheduledExecutor - Another thread pool, but often times could be a better fit than using java.util.concurrent.ScheduledThreadPoolExecutor. It offers a few advantages and disadvantages. Often times it can be better performing, or at least equally performing.

It does not handle any futures, nor does it implement ScheduledExecutorService by the default. If that functionality is necessary you have to wrap it in "ConcurrentSimpleSchedulerWrapper", but even then a few functions are not implemented due to wanting to keep the original implementation very lean and efficient.

It removes tasks much easier than ScheduledThreadPoolExecutor, by allowing you to just provide the original runnable, instead of requiring you to cast the future that was originally provided.

It offers the ability to submit tasks with a given priority. Low priority tasks will be less aggressive about making new threads, thus taking better advantage of it being a thread pool if the execution time does not have to be very precise. Using multiple priorities also reduces lock contention for higher priority tasks.

The other large difference compared to ScheduledThreadPoolExecutor is that the pool size is dynamic. In ScheduledThreadPoolExecutor you can only provide one size, and that pool can not grow or shrink. In this implementation you have a core size, max size, and workers can be expired if they are no longer used.

*    TaskExecutorDistributor and TaskSchedulerDistributor provide you the ability to execute (or schedule) tasks with a given key such that tasks with the same key hash code will NEVER run concurrently. This is designed as an ability to help the developer from having to deal with concurrent issues when ever possible. It allows you to have multiple runnables or tasks that share memory, but don't force the developer to deal with synchronization and memory barriers (assuming they all share the same key).

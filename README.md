Threadly
========

A library of java tools to assist with development of concurrent java applications. It includes a collection of tools to help with a wide range of development and testing needs. This is designed to be a complement to java.util.concurrent and uses java.util.concurrent to help assist in it's implementations where it makes sense. This library is particularly useful for getting legacy concurrent code and getting it under test.

-- Unit Test Tools --

*    TestablePriorityScheduler - Probably the largest gem in the tool kit. This scheduler is designed to take blocking and concurrent code and make it more controllable. It only allows one thread to execute at any given time. In addition threads are progressed forward by .tick() calls to the scheduler.

It relies on any code that performs blocking uses an injectable LockFactory, then when under test TestablePriorityScheduler can be provided as the lock factory. When actions like .wait .sleep occur on locks that the testable scheduler produced it will yield those threads and give control to other threads.

*    TestCondition - often times in doing unit test for asynchronous operations you have to wait for a condition to be come true. This class gives a way to easily wait for those conditions to be true, or throw an exception if they do not happen after a given timeout. The implementation of TestRunnable gives a good example of how this can be used.

*    TestRunnable - a runnable structure that has common operations already implemented. It gives two functions handleRunStart and handleRunFinish to allow people to optionally override to provide any test specific operation which is necessary. You can see many examples in our own unit test code of how we are using this.

-- General Concurrency Tools --

*    PriorityScheduledExecutor - Another thread pool, but often times could be a better fit than using java.util.concurrent.ScheduledThreadPoolExecutor. It offers a few advantages and disadvantages. Often times it can be better performing, or at least equally performing.

PriorityScheduledExecutor provides calls that do, and do not return a future, so if a future is not necessary the performance hit can be avoided.

If you need a thread pool that implements java.util.concurrent.ScheduledExecutorService you can wrap it in "PriorityScheduledExecutorServiceWrapper".

PriorityScheduledExecutor removes tasks much easier than java.util.concurrent.ScheduledThreadPoolExecutor, by allowing you to just provide the original runnable, instead of requiring you to cast the future that was originally provided.

It offers the ability to submit tasks with a given priority. Low priority tasks will be less aggressive about making new threads, thus taking better advantage of it being a thread pool if the execution time does not have to be very precise. Using multiple priorities also reduces lock contention for higher priority tasks.

The other large difference compared to ScheduledThreadPoolExecutor is that the pool size is dynamic. In ScheduledThreadPoolExecutor you can only provide one size, and that pool can not grow or shrink. In this implementation you have a core size, max size, and workers can be expired if they are no longer used.

*    ExecutorLimiter and PrioritySchedulerLimiter - These are designed so you can control the amount of concurrency in different parts of code, while still taking maximum benefit of having one large thread pool.

The design is such so that you create one large pool, and then wrap it in one of these two wrappers.  You then pass the wrapper to your different parts of code.  It relies on the large pool in order to actually get a thread, but this prevents any one section of code from completely dominating the thread pool.

*    TaskExecutorDistributor and TaskSchedulerDistributor provide you the ability to execute (or schedule) tasks with a given key such that tasks with the same key hash code will NEVER run concurrently. This is designed as an ability to help the developer from having to deal with concurrent issues when ever possible. It allows you to have multiple runnables or tasks that share memory, but don't force the developer to deal with synchronization and memory barriers (assuming they all share the same key).  These now also allow you to continue to use Future's with the key based execution.

*    CallableDistributor allows you to execute callables with a given key. All callables with the same key will never run concurrently (using TaskExecutorDistributor as the back end). Then another thread can easily reap the results from those callables by requesting them via known keys.

*    ConcurrentArrayList is a thread safe array list that also implements a Dequeue. It may be better performing than a CopyOnWriteArrayList depending on what the use case is. It is able to avoid copies for some operations, primarily adding and removing from the ends of a list (and can be tuned for the specific application to possibly make copies very rare).

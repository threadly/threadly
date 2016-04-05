package org.threadly.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * <p>A simple and light weight implementation of the {@link SchedulerService}.  This is designed 
 * to be a lighter weight option to the {@link PriorityScheduler}, for when multiple threads are 
 * either not needed, or not desired.<p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SingleThreadScheduler extends AbstractPriorityScheduler {
  protected final SchedulerManager sManager;
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.
   */
  public SingleThreadScheduler() {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public SingleThreadScheduler(TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
    this(defaultPriority, maxWaitForLowPriorityInMs, true);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadScheduler(boolean daemonThread) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, daemonThread);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadScheduler(TaskPriority defaultPriority, 
                               long maxWaitForLowPriorityInMs, boolean daemonThread) {
    this(defaultPriority, maxWaitForLowPriorityInMs, 
         new ConfigurableThreadFactory(SingleThreadScheduler.class.getSimpleName() + "-",
                                       true, daemonThread, Thread.NORM_PRIORITY, null, null));
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadScheduler(ThreadFactory threadFactory) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, threadFactory);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadScheduler(TaskPriority defaultPriority, 
                               long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    this(defaultPriority, 
         new SchedulerManager(defaultPriority, maxWaitForLowPriorityInMs, threadFactory));
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  This is for internal usage where different 
   * implementations of {@link SchedulerManager} need to be provided.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param SchedulerManager Scheduler manager to run against
   */
  protected SingleThreadScheduler(TaskPriority defaultPriority, SchedulerManager schedulerManager) {
    super(defaultPriority);
    
    this.sManager = schedulerManager;
  }
  
  /**
   * Gets the instance of the scheduler for this instance.  The scheduler must be accessed from 
   * this function because it is lazily constructed and started.  This call will verify the 
   * scheduler is running before it is returned
   * 
   * @return instance of the internal scheduler
   * @throws RejectedExecutionException thrown if the scheduler has been shutdown
   */
  protected NoThreadScheduler getRunningScheduler() throws RejectedExecutionException {
    sManager.startIfNotRunning();
    if (sManager.hasBeenStopped()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    return sManager.scheduler;
  }
  
  /**
   * Stops the scheduler, constructing if necessary.
   * 
   * @param stopImmediately if {@code true} after invocation no additional executions will occur
   * @return if stopped immediately a list of Runnables that were in queue at stop will be returned
   */
  private List<Runnable> shutdown(boolean stopImmediately) {
    return sManager.stop(stopImmediately);
  }

  /**
   * Stops any new tasks from being submitted to the pool.  But allows all tasks which are 
   * submitted to execute, or scheduled (and have elapsed their delay time) to run.  If recurring 
   * tasks are present they will also be unable to reschedule.  This call will not block to wait 
   * for the shutdown of the scheduler to finish.  If {@code shutdown()} or 
   * {@link #shutdownNow()} has already been called, this will have no effect.  
   * 
   * If you wish to not want to run any queued tasks you should use {@link #shutdownNow()}.
   */
  public void shutdown() {
    shutdown(false);
  }

  /**
   * Stops any new tasks from being submitted to the pool.  If any tasks are waiting for execution 
   * they will be prevented from being run.  If a task is currently running it will be allowed to 
   * finish (though this call will not block waiting for it to finish).
   * 
   * @return returns a list of runnables which were waiting in the queue to be run at time of shutdown
   */
  public List<Runnable> shutdownNow() {
    return shutdown(true);
  }

  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block forever.
   * 
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public void awaitTermination() throws InterruptedException {
    sManager.execThread.join();
  }

  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block until the 
   * timeout is reached.
   * 
   * @param timeoutMillis time to block and wait for thread pool to shutdown
   * @return {@code true} if the pool has shutdown, false if timeout was reached
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
    sManager.execThread.join(timeoutMillis);
    
    return ! sManager.execThread.isAlive();
  }

  @Override
  public boolean isShutdown() {
    return sManager.hasBeenStopped();
  }
  
  /**
   * One step beyond {@link #isShutdown()}, a {@code true} here indicates that not only has a 
   * shutdown on the pool been requested, but that the thread running tasks has completed.
   * 
   * @return {@code true} if this scheduler is no longer processing any tasks
   */
  public boolean isTerminated() {
    return sManager.hasBeenStopped() && ! sManager.execThread.isAlive();
  }
  
  /**
   * Start thread for execution if not already started.  This can avoid the minor delay of the 
   * thread starting later.  This will not block till the thread is running, instead letting it 
   * start in the background (usually fine).
   */
  public void prestartExecutionThread() {
    prestartExecutionThread(false);
  }

  /**
   * Start thread for execution if not already started.  This can avoid the minor delay of the 
   * thread starting later.
   * 
   * @param blockTillStarted If {@code true} this will not return till the scheduler thread has started 
   */
  public void prestartExecutionThread(boolean blockTillStarted) {
    if (sManager.startIfNotRunning()) {
      sManager.scheduler.execute(DoNothingRunnable.instance());
      if (blockTillStarted) {
        while (! sManager.execThread.isAlive() && sManager.state.get() < 1) {
          Thread.yield();
        }
      }
    }
  }

  @Override
  protected QueueManager getQueueManager() {
    return sManager.scheduler.queueManager;
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    return getRunningScheduler().doSchedule(task, delayInMillis, priority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    getRunningScheduler().scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    getRunningScheduler().scheduleAtFixedRate(task, initialDelay, period, priority);
  }
  
  @Override
  protected void finalize() {
    // if being GC'ed, stop thread so that it also can be GC'ed
    shutdown();
  }

  @Override
  public int getActiveTaskCount() {
    return sManager.scheduler.getActiveTaskCount();
  }
  
  /**
   * <p>This class contains the thread and instance of {@link NoThreadScheduler} that is used to 
   * provide single threaded scheduler implementation.  The only implementation here is to contain 
   * those objects, and know how to start and stop the scheduler.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.0.0
   */
  protected static class SchedulerManager implements Runnable {
    protected final NoThreadScheduler scheduler;
    protected final AtomicInteger state = new AtomicInteger(-1); // -1 = new, 0 = started, 1 = stopping, 2 = stopped
    protected final Thread execThread;
    
    public SchedulerManager(TaskPriority defaultPriority, 
                            long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
      this(new NoThreadScheduler(defaultPriority, maxWaitForLowPriorityInMs), threadFactory);
    }
    
    public SchedulerManager(NoThreadScheduler scheduler, ThreadFactory threadFactory) {
      ArgumentVerifier.assertNotNull(threadFactory, "threadFactory");
      
      this.scheduler = scheduler;
      execThread = threadFactory.newThread(this);
      if (execThread.isAlive()) {
        throw new IllegalThreadStateException();
      }
    }

    /**
     * Checks if the scheduler has been requested to at least start the shutdown sequence.  This 
     * may return {@code true} if the thread is still running, but should not accept more tasks 
     * unless this is returning {@code false}.
     * 
     * @return {@code true} if the scheduler has been stopped
     */
    public boolean hasBeenStopped() {
      return state.get() > 0;
    }

    /**
     * Starts the scheduler thread.  If it has already been started this will throw an 
     * {@link IllegalStateException}.
     * 
     * @return {@code true} if scheduler was started.
     */
    public boolean startIfNotRunning() {
      if (state.get() == -1 && state.compareAndSet(-1, 0)) {
        execThread.start();
        
        return true;
      }
      return false;
    }
    
    /**
     * Call to stop the thread which is running tasks.  If this has already been stopped this call 
     * will have no effect.  Regardless if true or false is passed in, running tasks will NOT be 
     * Interrupted or stopped.  True will only prevent ANY extra tasks from running, while a false 
     * will let tasks ready to run complete before shutting down.
     * 
     * @param stopImmediately {@code false} if the scheduler should let ready tasks run, 
     *                        {@code true} stops scheduler immediately
     * @return if {@code stopImmediately} is {@code true}, this will include tasks which were queued to run, 
     *         otherwise will be an empty list
     */
    public List<Runnable> stop(boolean stopImmediately) {
      int stateVal = state.get();
      while (stateVal < 1) {
        if (state.compareAndSet(stateVal, 1)) {
          // we finish the shutdown immediately if requested, or if it was never started
          if (stopImmediately || stateVal == -1) {
            return finishShutdown();
          } else {
            /* add to the end of the ready to execute queue a task which 
             * will finish the shutdown of the scheduler. 
             */
            scheduler.execute(new InternalRunnable() {
              @Override
              public void run() {
                finishShutdown();
              }
            });
          }
          
          break;
        } else {
          stateVal = state.get();
        }
      }
      
      return Collections.emptyList();
    }
    
    /**
     * Finishes shutdown process, and clears any tasks that remain in the queue.
     * 
     * @return a list of runnables which remained in the queue after shutdown
     */
    private List<Runnable> finishShutdown() {
      state.set(2);
      scheduler.cancelTick();
      
      return scheduler.clearTasks();
    }
    
    @Override
    public void run() {
      while (state.get() != 2) {
        try {
          scheduler.blockingTick(null);
        } catch (InterruptedException e) {
          // reset interrupted status
          Thread.interrupted();
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
        }
      }
    }
  }
}

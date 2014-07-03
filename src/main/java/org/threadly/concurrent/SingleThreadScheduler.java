package org.threadly.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.util.ExceptionUtils;

/**
 * <p>A simple and light weight implementation of the {@link SubmitterSchedulerInterface}.  
 * This is designed to be a lighter weight option to the {@link PriorityScheduler}, for 
 * when multiple threads are either not needed, or not desired.<p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SingleThreadScheduler extends AbstractSubmitterScheduler
                                   implements SchedulerServiceInterface {
  private static final AtomicInteger NEXT_THREAD_ID = new AtomicInteger(1);
  
  protected final ThreadFactory threadFactory;
  protected final AtomicReference<SchedulerManager> sManager;
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until 
   * the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.
   */
  public SingleThreadScheduler() {
    this(true);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until 
   * the first task is provided.
   * 
   * @param daemonThread true if scheduler thread should be a daemon thread
   */
  public SingleThreadScheduler(final boolean daemonThread) {
    this(new ThreadFactory() {
           private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
          
           @Override
           public Thread newThread(Runnable runnable) {
             Thread thread = defaultFactory.newThread(runnable);
             
             thread.setDaemon(daemonThread);
             thread.setName("SingleThreadScheduler-" + NEXT_THREAD_ID.getAndIncrement());
             
             return thread;
           }
         });
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until 
   * the first task is provided.
   * 
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadScheduler(ThreadFactory threadFactory) {
    if (threadFactory == null) {
      throw new IllegalArgumentException("Must provide thread factory");
    }
    
    sManager = new AtomicReference<SchedulerManager>(null);
    this.threadFactory = threadFactory;
  }
  
  /**
   * Gets the instance of the scheduler for this instance.  The scheduler 
   * must be accessed from this function because it is lazily constructed 
   * and started.
   * 
   * @return instance of the single threaded scheduler
   */
  protected NoThreadScheduler getScheduler() {
    // we lazily construct and start the manager
    SchedulerManager result = sManager.get();
    if (result == null) {
      result = new SchedulerManager(threadFactory);
      if (sManager.compareAndSet(null, result)) {
        // we are the one and only, so start now
        result.start();
      } else {
        result = sManager.get();
      }
    }
    
    if (result.isStopped()) {
      throw new IllegalStateException("Scheduler has been shutdown");
    }
    
    return result.scheduler;
  }
  
  /**
   * Stops the scheduler, constructing if necessary.
   * 
   * @param stopImmediately if true after call no additional executions will occur
   * @return if stopped immediately a list of Runnables that were in queue at stop will be returned
   */
  private List<Runnable> shutdown(boolean stopImmediately) {
    SchedulerManager sm = sManager.get();
    if (sm == null) {
      sm = new SchedulerManager(threadFactory);
      if (! sManager.compareAndSet(null, sm)) {
        sm = sManager.get();
      }
    }
    
    return sm.stop(stopImmediately);
  }

  /**
   * Stops any new tasks from being submitted to the pool.  But allows all tasks which are 
   * submitted to execute, or scheduled (and have elapsed their delay time) to run.  If 
   * recurring tasks are present they will also be unable to reschedule.  This call will 
   * not block to wait for the shutdown of the scheduler to finish.  If shutdown or 
   * shutdownNow has already been called, this will have no effect.
   * 
   * If you wish to not want to run any queued tasks you should use {#link shutdownNow()).
   */
  public void shutdown() {
    shutdown(false);
  }

  /**
   * Stops any new tasks from being submitted to the pool.  If any tasks are waiting for 
   * execution they will be prevented from being run.  If a task is currently running it 
   * will be allowed to finish (though this call will not block waiting for it to finish).
   * 
   * @return returns a list of runnables which were waiting in the queue to be run at time of shutdown
   */
  public List<Runnable> shutdownNow() {
    return shutdown(true);
  }

  @Override
  public boolean isShutdown() {
    SchedulerManager sm = sManager.get();
    if (sm != null) {
      return sm.isStopped();
    } else {
      // if not created yet, the not shutdown
      return false;
    }
  }

  @Override
  public boolean remove(Runnable task) {
    return getScheduler().remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return getScheduler().remove(task);
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    getScheduler().doSchedule(task, delayInMillis);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    getScheduler().scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }
  
  /**
   * <p>This class contains the thread and instance of {@link NoThreadScheduler} 
   * that is used to provide single threaded scheduler implementation.  The only 
   * implementation here is to contain those objects, and know how to start and 
   * stop the scheduler.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.0.0
   */
  protected static class SchedulerManager implements Runnable {
    protected final NoThreadScheduler scheduler;
    protected final Thread execThread;
    private final Object startStopLock;
    private boolean started;  // locked around startStopLock
    private volatile boolean shutdownStarted;
    private volatile boolean shutdownFinished;
    
    protected SchedulerManager(ThreadFactory threadFactory) {
      scheduler = new NoThreadScheduler(true);  // true so we wont tight loop in the run
      execThread = threadFactory.newThread(this);
      startStopLock = new Object();
      started = false;
      shutdownStarted = false;
      shutdownFinished = false;
    }
    
    /**
     * Call to check if stop has been called.
     * 
     * @return true if stop has been called (weather shutdown has finished or not)
     */
    public boolean isStopped() {
      return shutdownStarted;
    }
    
    /**
     * Call to start the thread to run tasks.  If already started this call will have 
     * no effect.
     */
    protected void start() {
      synchronized (startStopLock) {
        if (! started && ! shutdownStarted) {
          started = true;
          execThread.start();
        }
      }
    }
    
    /**
     * Call to stop the thread which is running tasks.  If this has already been stopped this call 
     * will have no effect.  Regardless if true or false is passed in, running tasks will NOT be 
     * Interrupted or stopped.  True will only prevent ANY extra tasks from running, while a false 
     * will let tasks ready to run complete before shutting down.
     * 
     * @param stopImmediately false if the scheduler should let ready tasks run, true stops scheduler immediately
     * @return if stopImmediately, this will include tasks which were queued to run, otherwise will be an empty list
     */
    protected List<Runnable> stop(boolean stopImmediately) {
      synchronized (startStopLock) {
        if (! shutdownStarted) {
          shutdownStarted = true;
          
          if (started) {
            if (stopImmediately) {
              return finishShutdown();
            } else {
              /* add to the end of the ready to execute queue a task which 
               * will finish the shutdown of the scheduler. 
               */
              scheduler.execute(new Runnable() {
                @Override
                public void run() {
                  finishShutdown();
                }
              });
            }
          }
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
      shutdownFinished = true;
      scheduler.cancelTick();
      
      return scheduler.clearTasks();
    }
    
    @Override
    public void run() {
      while (! shutdownFinished) {
        try {
          scheduler.tick();
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

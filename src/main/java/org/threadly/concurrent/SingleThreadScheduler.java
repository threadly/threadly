package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.util.ExceptionUtils;

/**
 * <p>A simple and light weight implementation of the {@link SubmitterSchedulerInterface}.  
 * This is designed to be a lighter weight option to the {@link PriorityScheduledExecutor}, 
 * for when multiple threads are either not needed, or not desired.<p>
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
    
    if (result.stopped) {
      throw new IllegalStateException("Scheduler has been shutdown");
    }
    
    return result.scheduler;
  }
  
  /**
   * Stops the scheduler from running more tasks.  Because of how the 
   * {@link NoThreadScheduler} works, this wont actually any tasks 
   * till after the current tick call finishes.
   */
  public void shutdown() {
    SchedulerManager sm = sManager.get();
    if (sm == null) {
      sm = new SchedulerManager(threadFactory);
      if (! sManager.compareAndSet(null, sm)) {
        sm = sManager.get();
      }
    }
    
    sm.stop();
  }

  @Override
  public boolean isShutdown() {
    SchedulerManager sm = sManager.get();
    if (sm != null) {
      return sm.stopped;
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
    private volatile boolean stopped;
    private boolean started;
    
    protected SchedulerManager(ThreadFactory threadFactory) {
      scheduler = new NoThreadScheduler(true);  // true so we wont tight loop in the run
      execThread = threadFactory.newThread(this);
      startStopLock = new Object();
      started = false;
      stopped = false;
    }
    
    public boolean isStopped() {
      return stopped;
    }
    
    public void start() {
      synchronized (startStopLock) {
        if (! started && ! stopped) {
          started = true;
          execThread.start();
        }
      }
    }
    
    public void stop() {
      synchronized (startStopLock) {
        stopped = true;
        
        if (started) {
          // send interrupt to unblock if currently waiting for tasks
          execThread.interrupt();
        }
      }
    }
    
    @Override
    public void run() {
      while (! stopped) {
        try {
          scheduler.tick();
        } catch (RuntimeException e) {
          ExceptionUtils.handleException(e);
        } catch (InterruptedException e) {
          // reset interrupted status
          Thread.interrupted();
        }
      }
    }
  }
}

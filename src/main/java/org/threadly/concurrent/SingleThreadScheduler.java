package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ExceptionUtils;

/**
 * <p>A simple and light weight implementation of the {@link SubmitterSchedulerInterface}.  
 * This is designed to be a lighter weight option to the {@link PriorityScheduledExecutor}, 
 * for when multiple threads are either not needed, or not desired.<p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SingleThreadScheduler implements SchedulerServiceInterface {
  private static final AtomicInteger NEXT_THREAD_ID = new AtomicInteger(1);
  
  private final AtomicReference<SchedulerManager> sManager;
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until 
   * the first task is provided.
   */
  public SingleThreadScheduler() {
    sManager = new AtomicReference<SchedulerManager>(null);
  }
  
  /**
   * Gets the instance of the scheduler for this instance.  The scheduler 
   * must be accessed from this function because it is lazily constructed 
   * and started.
   * 
   * @return instance of the single threaded scheduler
   */
  protected SchedulerServiceInterface getScheduler() {
    // we lazily construct and start the manager
    SchedulerManager result = sManager.get();
    if (result == null) {
      result = new SchedulerManager();
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
   * {@link AbstractTickableScheduler} works, this wont actually any 
   * tasks till after the current tick call finishes.
   */
  public void shutdown() {
    SchedulerManager sm = sManager.get();
    if (sm == null) {
      sm = new SchedulerManager();
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
  public void schedule(Runnable task, long delayInMs) {
    getScheduler().schedule(task, delayInMs);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    getScheduler().scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  @Override
  public void execute(Runnable command) {
    getScheduler().execute(command);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return getScheduler().submit(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return getScheduler().submit(task, result);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return getScheduler().submit(task);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return getScheduler().submitScheduled(task, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return getScheduler().submitScheduled(task, result, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    return getScheduler().submitScheduled(task, delayInMs);
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
    private final NoThreadScheduler scheduler;
    private final Thread execThread;
    private final Object startStopLock;
    private volatile boolean stopped;
    private boolean started;
    
    protected SchedulerManager() {
      scheduler = new NoThreadScheduler(true);  // true so we wont tight loop in the run
      execThread = new Thread(this);
      execThread.setName("SingleThreadScheduler-" + NEXT_THREAD_ID.getAndIncrement());
      startStopLock = new Object();
      started = false;
      stopped = false;
    }
    
    public void start() {
      synchronized (startStopLock) {
        if (! started && ! stopped) {
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

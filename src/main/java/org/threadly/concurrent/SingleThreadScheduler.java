package org.threadly.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * <p>A simple and light weight implementation of the {@link SubmitterSchedulerInterface}.  This 
 * is designed to be a lighter weight option to the {@link PriorityScheduler}, for when multiple 
 * threads are either not needed, or not desired.<p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SingleThreadScheduler extends AbstractSubmitterScheduler
                                   implements SchedulerServiceInterface {
  protected final ThreadFactory threadFactory;
  protected final AtomicReference<SchedulerManager> sManager;
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.
   */
  public SingleThreadScheduler() {
    this(true);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadScheduler(boolean daemonThread) {
    this(new ConfigurableThreadFactory(SingleThreadScheduler.class.getSimpleName() + "-",
                                       true, daemonThread, Thread.NORM_PRIORITY, null, null));
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadScheduler(ThreadFactory threadFactory) {
    ArgumentVerifier.assertNotNull(threadFactory, "threadFactory");
    
    sManager = new AtomicReference<SchedulerManager>(null);
    this.threadFactory = threadFactory;
  }
  
  /**
   * Returns the {@link SchedulerManager} instance that contains the scheduler.  This does no 
   * safety checks on if the scheduler is running or not, it just ensures that the single instance 
   * of the manager is returned.
   * 
   * @return Single instance of the SchedulerManager container
   */
  private SchedulerManager getSchedulerManager() {
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
    
    return result;
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
    SchedulerManager result = getSchedulerManager();
    
    if (! result.isRunning()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    return result.scheduler;
  }
  
  /**
   * Stops the scheduler, constructing if necessary.
   * 
   * @param stopImmediately if {@code true} after invocation no additional executions will occur
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

  @Override
  public boolean isShutdown() {
    SchedulerManager sm = sManager.get();
    if (sm != null) {
      return ! sm.isRunning();
    } else {
      // if not created yet, the not shutdown
      return false;
    }
  }

  @Override
  public boolean remove(Runnable task) {
    return getSchedulerManager().scheduler.remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return getSchedulerManager().scheduler.remove(task);
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    getRunningScheduler().doSchedule(task, delayInMillis);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    getRunningScheduler().scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    getRunningScheduler().scheduleAtFixedRate(task, initialDelay, period);
  }
  
  /**
   * <p>This class contains the thread and instance of {@link NoThreadScheduler} that is used to 
   * provide single threaded scheduler implementation.  The only implementation here is to contain 
   * those objects, and know how to start and stop the scheduler.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.0.0
   */
  protected static class SchedulerManager extends AbstractService 
                                          implements Runnable {
    protected final NoThreadScheduler scheduler;
    protected final Thread execThread;
    private volatile boolean shutdownFinished;
    
    protected SchedulerManager(ThreadFactory threadFactory) {
      scheduler = new NoThreadScheduler(true);  // true so we wont tight loop in the run
      execThread = threadFactory.newThread(this);
      if (execThread.isAlive()) {
        throw new IllegalThreadStateException();
      }
      shutdownFinished = false;
    }

    @Override
    protected void startupService() {
      execThread.start();
    }

    @Override
    protected void shutdownService() {
      // nothing to shutdown here, shutdown actions should be done in stop(boolean)
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
    protected List<Runnable> stop(boolean stopImmediately) {
      if (stopIfRunning()) {
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
          scheduler.tick(null);
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

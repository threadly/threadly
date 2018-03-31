package org.threadly.concurrent.wrapper.limiter;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.threadly.concurrent.ContainerHelper;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.future.ListenableFuture;

/**
 * This class is designed to limit how much parallel execution happens on a provided 
 * {@link SchedulerService}.  This allows the implementor to have one thread pool for all 
 * their code, and if they want certain sections to have less levels of parallelism (possibly 
 * because those those sections would completely consume the global pool), they can wrap the 
 * executor in this class.
 * <p>
 * Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.
 * <p>
 * This is an alternative from having to create multiple thread pools.  By using this you also are 
 * able to accomplish more efficiently thread use than multiple thread pools would.
 * <p>
 * This extends the {@link SubmitterSchedulerLimiter} to add {@link SchedulerService} features.  
 * This does not cause any performance hits, but does require a source {@link SchedulerService} to 
 * rely on.  If you have a {@link SchedulerService} available this implementation should be 
 * preferred over the {@link SubmitterSchedulerLimiter}.
 * <p>
 * If limiting to a single thread, please see {@link SingleThreadSchedulerSubPool} as a possible 
 * alternative.
 * 
 * @since 4.6.0 (since 2.0.0 at org.threadly.concurrent.limiter)
 */
public class SchedulerServiceLimiter extends SubmitterSchedulerLimiter
                                     implements SchedulerService {
  protected final SchedulerService scheduler;
  private final Collection<WeakReference<RecurringWrapper>> recurringTasks;
  
  /**
   * Constructs a new limiter that implements the {@link SchedulerService}.
   * 
   * @param scheduler {@link SchedulerService} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public SchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency) {
    this(scheduler, maxConcurrency, DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SchedulerService}.
   * <p>
   * This constructor allows you to specify if listeners / 
   * {@link org.threadly.concurrent.future.FutureCallback}'s / functions in 
   * {@link ListenableFuture#map(java.util.function.Function)} or 
   * {@link ListenableFuture#flatMap(java.util.function.Function)} should be counted towards the 
   * concurrency limit.  Specifying {@code false} will release the limit as soon as the original 
   * task completes.  Specifying {@code true} will continue to enforce the limit until all listeners 
   * (without an executor) complete.
   * 
   * @param scheduler {@link SchedulerService} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   */
  public SchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency, 
                                 boolean limitFutureListenersExecution) {
    super(scheduler, maxConcurrency, limitFutureListenersExecution);
    
    this.scheduler = scheduler;
    this.recurringTasks = new ConcurrentLinkedQueue<>();
  }
  
  @Override
  protected void initialRecurringSchedule(RecurringWrapper rw, long initialDelay) {
    // first cleanup if needed
    Iterator<WeakReference<RecurringWrapper>> it = recurringTasks.iterator();
    while (it.hasNext()) {
      if (it.next().get() == null) {
        it.remove();
      }
    }

    recurringTasks.add(new WeakReference<>(rw));
    
    super.initialRecurringSchedule(rw, initialDelay);
  }

  @Override
  public boolean remove(Runnable task) {
    Iterator<WeakReference<RecurringWrapper>> it = recurringTasks.iterator();
    while (it.hasNext()) {
      RecurringWrapper rw = it.next().get();
      if (rw == null) {
        it.remove();
      } else if (ContainerHelper.isContained(rw, task)) {
        it.remove();
        
        if (rw.invalidate()) {
          // try to remove proactively
          waitingTasks.remove(rw);
          scheduler.remove(rw.delayRunnable);
          scheduler.remove(rw);
          return true;
        }
      }
    }
    
    // synchronize on this so that we don't consume tasks while trying to remove
    synchronized (this) {
      return ContainerHelper.remove(waitingTasks, task) || scheduler.remove(task);
    }
  }

  @Override
  public boolean remove(Callable<?> task) {
    Iterator<WeakReference<RecurringWrapper>> it = recurringTasks.iterator();
    while (it.hasNext()) {
      RecurringWrapper rw = it.next().get();
      if (rw == null) {
        it.remove();
      } else if (ContainerHelper.isContained(rw, task)) {
        it.remove();
        
        if (rw.invalidate()) {
          // try to remove proactively
          waitingTasks.remove(rw);
          scheduler.remove(rw.delayRunnable);
          scheduler.remove(rw);
          return true;
        }
      }
    }
    
    // synchronize on this so that we don't consume tasks while trying to remove
    synchronized (this) {
      return ContainerHelper.remove(waitingTasks, task) || scheduler.remove(task);
    }
  }

  @Override
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }

  @Override
  public int getQueuedTaskCount() {
    return scheduler.getQueuedTaskCount() + waitingTasks.size();
  }

  @Override
  public int getWaitingForExecutionTaskCount() {
    return scheduler.getWaitingForExecutionTaskCount() + waitingTasks.size();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}

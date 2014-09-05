package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.ContainerHelper;
import org.threadly.concurrent.SchedulerServiceInterface;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link SchedulerServiceInterface}.  This allows the implementor to have one thread pool for all 
 * their code, and if they want certain sections to have less levels of parallelism (possibly 
 * because those those sections would completely consume the global pool), they can wrap the 
 * executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.</p>
 * 
 * <p>This is an alternative from having to create multiple thread pools.  By using this you also 
 * are able to accomplish more efficiently thread use than multiple thread pools would.</p>
 * 
 * <p>This extends the {@link SimpleSchedulerLimiter} to add {@link SchedulerServiceInterface} 
 * features.  This does not cause any performance hits, but does require a source 
 * {@link SchedulerServiceInterface} to rely on.  If you have a {@link SchedulerServiceInterface} 
 * available this implementation should be preferred over the {@link SimpleSchedulerLimiter}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SchedulerServiceLimiter extends SimpleSchedulerLimiter
                                     implements SchedulerServiceInterface {
  protected final SchedulerServiceInterface scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link SchedulerServiceInterface}.
   * 
   * @param scheduler {@link SchedulerServiceInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public SchedulerServiceLimiter(SchedulerServiceInterface scheduler, 
                                 int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SchedulerServiceInterface}.
   * 
   * @param scheduler {@link SchedulerServiceInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  public SchedulerServiceLimiter(SchedulerServiceInterface scheduler, 
                                 int maxConcurrency, String subPoolName) {
    super(scheduler, maxConcurrency, subPoolName);
    
    this.scheduler = scheduler;
  }

  @Override
  public boolean remove(Runnable task) {
    // synchronize on this so that we don't consume tasks while trying to remove
    synchronized (this) {
      // try to remove from scheduler first
      if (scheduler.remove(task)) {
        return true;
      }
      
      return ContainerHelper.remove(waitingTasks, task);
    }
  }

  @Override
  public boolean remove(Callable<?> task) {
    // synchronize on this so that we don't consume tasks while trying to remove
    synchronized (this) {
      // try to remove from scheduler first
      if (scheduler.remove(task)) {
        return true;
      }
      
      return ContainerHelper.remove(waitingTasks, task);
    }
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}

package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.ContainerHelper;
import org.threadly.concurrent.SchedulerService;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link SchedulerService}.  This allows the implementor to have one thread pool for all 
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
 * <p>This extends the {@link SubmitterSchedulerLimiter} to add {@link SchedulerService} 
 * features.  This does not cause any performance hits, but does require a source 
 * {@link SchedulerService} to rely on.  If you have a {@link SchedulerService} 
 * available this implementation should be preferred over the {@link SubmitterSchedulerLimiter}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SchedulerServiceLimiter extends SubmitterSchedulerLimiter
                                     implements SchedulerService {
  protected final SchedulerService scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link SchedulerService}.
   * 
   * @param scheduler {@link SchedulerService} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public SchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency) {
    super(scheduler, maxConcurrency);
    
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
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }

  @Override
  public int getQueuedTaskCount() {
    return scheduler.getQueuedTaskCount() + waitingTasks.size();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}

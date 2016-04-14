package org.threadly.concurrent.limiter;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterface;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link SimpleSchedulerInterface}.  This allows the implementor to have one thread pool for all 
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
 * @deprecated Use {@link org.threadly.concurrent.wrapper.limiter.SubmitterSchedulerLimiter} as an alternative
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
@Deprecated
public class SimpleSchedulerLimiter extends SubmitterSchedulerLimiter 
                                    implements SubmitterSchedulerInterface {
  protected final SimpleSchedulerInterface scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link SimpleSchedulerInterface}.
   * 
   * @param scheduler {@link SimpleSchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public SimpleSchedulerLimiter(SimpleSchedulerInterface scheduler, int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SimpleSchedulerInterface}.
   * 
   * @param scheduler {@link SimpleSchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  public SimpleSchedulerLimiter(final SimpleSchedulerInterface scheduler, 
                                int maxConcurrency, String subPoolName) {
    super(scheduler instanceof SubmitterScheduler ? 
            (SubmitterScheduler)scheduler : scheduler == null ? null : new AbstractSubmitterScheduler() {
      @Override
      public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
        scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
      }

      @Override
      public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
        scheduler.scheduleAtFixedRate(task, initialDelay, period);
      }

      @Override
      protected void doSchedule(Runnable task, long delayInMs) {
        scheduler.schedule(task, delayInMs);
      }
    }, maxConcurrency, subPoolName);
    
    this.scheduler = scheduler;
  }
}

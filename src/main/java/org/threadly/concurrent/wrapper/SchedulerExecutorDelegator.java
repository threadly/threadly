package org.threadly.concurrent.wrapper;

import java.util.concurrent.Executor;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.util.ArgumentVerifier;

/**
 * Class which takes in both an executor and a scheduler.  Delegating executions to the executor 
 * when possible, and otherwise submitting to the provided scheduler.  This can be used to provide 
 * different behavior/implementations between scheduled and executed tasks (for example you could 
 * use {@link org.threadly.concurrent.wrapper.priority.DefaultPriorityWrapper} to 
 * have a different default priority for scheduled tasks vs executed).
 * 
 * @since 4.7.0
 */
public class SchedulerExecutorDelegator extends AbstractSubmitterScheduler {
  protected final Executor parentExecutor;
  protected final SubmitterScheduler parentScheduler;
  
  /**
   * Constructs a new delegator with the provided pools to defer executions to.
   * 
   * @param parentExecutor Executor to use when ever possible
   * @param parentScheduler Scheduler to use when executions need to be delayed
   */
  public SchedulerExecutorDelegator(Executor parentExecutor, 
                                    SubmitterScheduler parentScheduler) {
    ArgumentVerifier.assertNotNull(parentExecutor, "parentExecutor");
    ArgumentVerifier.assertNotNull(parentScheduler, "parentScheduler");
    
    this.parentExecutor = parentExecutor;
    this.parentScheduler = parentScheduler;
  }
  
  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    parentScheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }
  
  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    parentScheduler.scheduleAtFixedRate(task, initialDelay, period);
  }
  
  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    if (delayInMillis > 0) {
      parentScheduler.schedule(task, delayInMillis);
    } else {
      parentExecutor.execute(task);
    }
  }
}

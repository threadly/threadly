package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Executor;

import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.util.ExceptionUtils;

/**
 * This sub-pool is a special type of limiter.  It is able to have expanded semantics than the pool 
 * it delegates to.  For example this pool provides {@link TaskPriority} capabilities even though 
 * the pool it runs on top of does not necessarily provide that.  In addition most status's returned 
 * do not consider the parent pools state (for example {@link #getActiveTaskCount()} does not 
 * reflect the active tasks in the parent pool).
 * <p>
 * Most importantly difference in this "sub-pool" vs "limiter" is the way task execution order is 
 * maintained in the delegate pool.  In a limiter tasks will need to queue individually against the 
 * other tasks the delegate pool needs to execute.  In this implementation the sub-pool basically 
 * gets CPU time and it will attempt to execute everything it needs to.  It will not return the 
 * thread to the delegate pool until there is nothing left to process.
 * <p>
 * There are two big reasons you might want to use this sub pool over a limiter.  As long as the 
 * above details are not problematic, this is a much more efficient implementation.  Providing 
 * better load characteristics for submitting tasks, as well reducing the burden on the delegate 
 * pool.  In addition if you need limiter + priority capabilities, this is your only option.
 * 
 * @since 5.7
 */
public class SingleThreadSchedulerSubPool extends AbstractPriorityScheduler {
  private final SchedulerService delegateScheduler;
  private final NoThreadScheduler noThreadScheduler;
  private final TickTask tickTask;
  
  /**
   * Construct a new single threaded sub-pool.
   * 
   * @param delegateScheduler Scheduler to gain CPU time for task execution
   */
  public SingleThreadSchedulerSubPool(SchedulerService delegateScheduler) {
    this(delegateScheduler, TaskPriority.High, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  /**
   * 
   * Construct a new single threaded sub-pool with default task priority behaviors.
   * 
   * @param delegateScheduler Scheduler to gain CPU time for task execution
   * @param defaultPriority Default priority for tasks submitted to this pool
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public SingleThreadSchedulerSubPool(SchedulerService delegateScheduler, 
                                      TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
    super(defaultPriority);
    
    this.delegateScheduler = delegateScheduler;
    this.noThreadScheduler = new NoThreadScheduler(defaultPriority, maxWaitForLowPriorityInMs);
    this.tickTask = new TickTask(delegateScheduler);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    noThreadScheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
    delegateScheduler.scheduleWithFixedDelay(() -> tickTask.signalToRunImmediately(true), 
                                             initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    noThreadScheduler.scheduleAtFixedRate(task, initialDelay, period, priority);
    delegateScheduler.scheduleAtFixedRate(() -> tickTask.signalToRunImmediately(true), 
                                          initialDelay, period);
  }

  @Override
  public int getActiveTaskCount() {
    return noThreadScheduler.getActiveTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return delegateScheduler.isShutdown();
  }

  @Override
  protected QueueManager getQueueManager() {
    return noThreadScheduler.getQueueManager();
  }
  
  protected void executeTasks() {
    noThreadScheduler.tick(ExceptionUtils::handleException);
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    OneTimeTaskWrapper result = noThreadScheduler.doSchedule(task, delayInMillis, priority);
    if (delayInMillis > 0) {
      delegateScheduler.schedule(() -> tickTask.signalToRunImmediately(true), delayInMillis);
    } else {
      tickTask.signalToRun();
    }
    return result;
  }
  
  /**
   * Operation that should be signaled to run when there is something to execute on the 
   * NoThreadScheduler.  This will ensure that the scheduler is ticked in a single threaded manner.
   */
  private class TickTask extends ReschedulingOperation {
    protected TickTask(Executor delegateExecutor) {
      super(Integer.MAX_VALUE, delegateExecutor);
    }

    @Override
    protected void run() {
      executeTasks();
    }
  }
  
  /**
   * Extending class to gain visibility into protected functions from outside of the source package.
   */
  private static class NoThreadScheduler extends org.threadly.concurrent.NoThreadScheduler {
    public NoThreadScheduler(TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
      super(defaultPriority, maxWaitForLowPriorityInMs);
    }

    @Override
    protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
      return super.doSchedule(task, delayInMillis, priority);
    }
    
    @Override
    protected QueueManager getQueueManager() {
      return super.getQueueManager();
    }
  }
}

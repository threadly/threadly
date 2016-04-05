package org.threadly.concurrent.wrapper.traceability;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.SubmitterScheduler;

/**
 * <p>Class which wraps a {@link SubmitterScheduler} and wraps all supplied tasks in a 
 * {@link ThreadRenamingRunnableWrapper}.  This allows you to make a pool where all tasks submitted 
 * inside it have the threads named in an identifiable way.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
public class ThreadRenamingSubmitterSchedulerWrapper extends AbstractSubmitterScheduler {
  protected final SubmitterScheduler scheduler;
  protected final String threadName;
  protected final boolean replace;

  /**
   * Constructs a new {@link ThreadRenamingSubmitterSchedulerWrapper}, wrapping a supplied 
   * {@link SubmitterScheduler}.  If /{@code replace} is {@code false} the thread will be named such 
   * that {@code threadName[originalThreadName]}.
   * 
   * @param scheduler SubmitterScheduler to wrap and send executions to
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingSubmitterSchedulerWrapper(SubmitterScheduler scheduler, 
                                                 String threadName, boolean replace) {
    this.scheduler = scheduler;
    this.threadName = threadName;
    this.replace = replace;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduler.scheduleWithFixedDelay(new ThreadRenamingRunnableWrapper(task, threadName, replace), 
                                     initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduler.scheduleAtFixedRate(new ThreadRenamingRunnableWrapper(task, threadName, replace), 
                                  initialDelay, period);
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    scheduler.schedule(new ThreadRenamingRunnableWrapper(task, threadName, replace), delayInMillis);
  }
}
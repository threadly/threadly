package org.threadly.concurrent.wrapper.traceability;

import org.threadly.concurrent.SubmitterScheduler;

/**
 * <p>Class which wraps a {@link SubmitterScheduler} and wraps all supplied tasks in a 
 * {@link ThreadRenamingRunnable}.  This allows you to make a pool where all tasks submitted 
 * inside it have the threads named in an identifiable way.</p>
 * 
 * @deprecated Renamed to {@link ThreadRenamingSubmitterScheduler}
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent)
 */
@Deprecated
public class ThreadRenamingSubmitterSchedulerWrapper extends ThreadRenamingSubmitterScheduler {
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
    super(scheduler, threadName, replace);
  }
}
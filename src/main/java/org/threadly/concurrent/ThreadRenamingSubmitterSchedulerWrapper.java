package org.threadly.concurrent;

/**
 * <p>Class which wraps a {@link SubmitterScheduler} and wraps all supplied tasks in a 
 * {@link ThreadRenamingRunnableWrapper}.  This allows you to make a pool where all tasks submitted 
 * inside it have the threads named in an identifiable way.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.traceability.ThreadRenamingSubmitterSchedulerWrapper}
 * 
 * @author jent
 * @since 4.3.0
 */
@Deprecated
public class ThreadRenamingSubmitterSchedulerWrapper 
                 extends org.threadly.concurrent.wrapper.traceability.ThreadRenamingSubmitterSchedulerWrapper {
  /**
   * Constructs a new {@link ThreadRenamingSubmitterSchedulerWrapper}, wrapping a supplied 
   * {@link SchedulerService}.  If /{@code replace} is {@code false} the thread will be named such 
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
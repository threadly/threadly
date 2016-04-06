package org.threadly.concurrent.wrapper.intercepter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>Class to wrap {@link SubmitterScheduler} pool so that tasks can be intercepted and either 
 * wrapped, or modified, before being submitted to the pool.  This abstract class needs to have 
 * {@link #wrapTask(Runnable, boolean)} overridden to provide the task which should be submitted to the 
 * {@link SubmitterScheduler}.  Please see the javadocs of {@link #wrapTask(Runnable, boolean)} for 
 * more details about ways a task can be modified or wrapped.</p>
 * 
 * <p>Other variants of task wrappers: {@link ExecutorTaskIntercepter}, 
 * {@link SchedulerServiceTaskIntercepter}, {@link PrioritySchedulerTaskIntercepter}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0
 */
public abstract class SubmitterSchedulerTaskIntercepter extends ExecutorTaskIntercepter 
                                                        implements SubmitterScheduler {
  protected final SubmitterScheduler parentScheduler;
  
  protected SubmitterSchedulerTaskIntercepter(SubmitterScheduler parentScheduler) {
    super(parentScheduler);
    
    this.parentScheduler = parentScheduler;
  }

  /**
   * Overridden version which delegates to {@link #wrapTask(Runnable, boolean)}.  There should be 
   * no reason to override this, instead just ensure that {@link #wrapTask(Runnable, boolean)} is 
   * implemented.
   */
  @Override
  public final Runnable wrapTask(Runnable task) {
    return wrapTask(task, false);
  }
  
  /**
   * Implementation to modify a provided task.  The provided runnable will be the one submitted to 
   * the Executor, unless a {@link Callable} was submitted, in which case a 
   * {@link ListenableFutureTask} will be provided.  In the last condition the original callable 
   * can be retrieved by invoking {@link ListenableFutureTask#getContainedCallable()}.  The returned 
   * task can not be null, but could be either the original task, a modified task, a wrapper to the 
   * provided task, or if no action is desired 
   * {@link org.threadly.concurrent.DoNothingRunnable#instance()} may be provided.  However caution 
   * should be used in that if a {@link ListenableFutureTask} is provided, and then never returned 
   * (and not canceled), then the future will never complete (and thus possibly forever blocked).  
   * So if you are doing conditional checks for {@link ListenableFutureTask} and may not 
   * execute/return the provided task, then you should be careful to ensure 
   * {@link ListenableFutureTask#cancel(boolean)} is invoked.
   * 
   * Public visibility for javadoc visibility.
   * 
   * @param task A runnable that was submitted for execution
   * @param recurring {@code true} if the provided task is a recurring task
   * @return A non-null task that will be provided to the parent executor
   */
  public abstract Runnable wrapTask(Runnable task, boolean recurring);

  @Override
  public void schedule(Runnable task, long delayInMs) {
    parentScheduler.schedule(task == null ? null : wrapTask(task, false), delayInMs);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return parentScheduler.submitScheduled(task == null ? null : wrapTask(task, false), 
                                           result, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);

    parentScheduler.schedule(wrapTask(lft, false), delayInMs);
    
    return lft;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    parentScheduler.scheduleWithFixedDelay(task == null ? null : wrapTask(task, true), 
                                           initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    parentScheduler.scheduleAtFixedRate(task == null ? null : wrapTask(task, true), 
                                        initialDelay, period);
  }
}

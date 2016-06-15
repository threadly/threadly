package org.threadly.concurrent.wrapper.interceptor;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>Class to wrap {@link Executor} pool so that tasks can be intercepted and either wrapped, or 
 * modified, before being submitted to the pool. This class can be passed a lamba in the 
 * constructor, or you can extend this class and override the function {@link #wrapTask(Runnable)}
 * to provide the task which should be submitted to the {@link Executor}.  Please see the javadocs
 * of {@link #wrapTask(Runnable)} for more details about ways a task can be modified or wrapped.
 * </p>
 * 
 * <p>Other variants of task wrappers: {@link SubmitterSchedulerTaskInterceptor}, 
 * {@link SchedulerServiceTaskInterceptor}, {@link PrioritySchedulerTaskInterceptor}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0
 */
public class ExecutorTaskInterceptor implements SubmitterExecutor {
  protected final Executor parentExecutor;
  protected final Function<Runnable, Runnable> taskManipulator;
  
  /**
   * When using this constructor, {@link #wrapTask(Runnable)} must be overridden to handle the 
   * task manipulation before the task is submitted to the provided {@link Executor}.  Please see 
   * the javadocs of {@link #wrapTask(Runnable)} for more details about ways a task can be 
   * modified or wrapped.
   * 
   * @param parentExecutor An instance of {@link Executor} to wrap
   */
  protected ExecutorTaskInterceptor(Executor parentExecutor) {
    this(parentExecutor, null);
  }
  
  /**
   * Constructs a wrapper for {@link Executor} pool so that tasks can be intercepted and modified,
   * before being submitted to the pool.
   * 
   * @param parentExecutor An instance of {@link Executor} to wrap
   * @param taskManipulator A lambda to manipulate a {@link Runnable} that was submitted for execution
   */
  public ExecutorTaskInterceptor(Executor parentExecutor, Function<Runnable, Runnable> taskManipulator) {
    ArgumentVerifier.assertNotNull(parentExecutor, "parentExecutor");
    
    this.taskManipulator = taskManipulator;
    this.parentExecutor = parentExecutor;
  }
  
  /**
   * Implementation to modify a provided task.  The provided runnable will be the one submitted to 
   * the Executor, unless a {@link Callable} was submitted, in which case a 
   * {@link ListenableFutureTask} will be provided. In the last condition the original callable 
   * can be retrieved by invoking {@link ListenableFutureTask#getContainedCallable()}. The returned 
   * task can not be null, but could be either the original task, a modified task, a wrapper to the 
   * provided task, or if no action is desired 
   * {@link org.threadly.concurrent.DoNothingRunnable#instance()} may be provided.  However caution 
   * should be used in that if a {@link ListenableFutureTask} is provided, and then never returned 
   * (and not canceled), then the future will never complete (and thus possibly forever blocked).  
   * So if you are doing conditional checks for {@link ListenableFutureTask} and may not 
   * execute/return the provided task, then you should be careful to ensure 
   * {@link ListenableFutureTask#cancel(boolean)} is invoked.
   * 
   * @param task A runnable that was submitted for execution
   * @return A non-null task that will be provided to the parent executor
   */
  public Runnable wrapTask(Runnable task) {
    return this.taskManipulator.apply(task);
  }

  @Override
  public void execute(Runnable task) {
    parentExecutor.execute(task == null ? null : wrapTask(task));
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, wrapTask(task), result);
    
    parentExecutor.execute(lft);
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);

    parentExecutor.execute(wrapTask(lft));
    
    return lft;
  }
}

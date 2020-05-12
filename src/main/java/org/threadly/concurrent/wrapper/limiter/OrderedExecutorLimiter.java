package org.threadly.concurrent.wrapper.limiter;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ArgumentVerifier;

/**
 * Implementation of {@link ExecutorLimiter} which allows you to order the tasks in a way other 
 * than FIFO.  If first in first out is acceptable {@link ExecutorLimiter} is a superior option.
 * 
 * Please see the constructor javadocs for more details about sorting: 
 *  {@link #OrderedExecutorLimiter(Executor, int, Comparator)}
 * 
 * @param <T> Type of task that can be used for comparison
 * @since 6.2
 */
public class OrderedExecutorLimiter<T extends Runnable> {
  private static final int INITIAL_QUEUE_SIZE = 16;
  
  protected final ExecutorLimiter limiter;
  
  /**
   * Construct a new limiter, providing the implementation of how tasks should be sorted relative 
   * to each other.  The {@link Comparator} provided must deterministically provide ordering such 
   * that two tasks must always have the same relative order.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of tasks to run in parallel
   * @param sorter Implementation of {@link Comparator} to sort the task queue being limited
   */
  @SuppressWarnings("unchecked")
  public OrderedExecutorLimiter(Executor executor, int maxConcurrency, 
                                final Comparator<? super T> sorter) {
    this(executor, maxConcurrency, ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION, sorter);
  }
  
  /**
   * Construct a new limiter, providing the implementation of how tasks should be sorted relative 
   * to each other.  The {@link Comparator} provided must deterministically provide ordering such 
   * that two tasks must always have the same relative order.
   * <p>
   * This constructor allows you to specify if listeners / 
   * {@link org.threadly.concurrent.future.FutureCallback}'s / functions in 
   * {@link ListenableFuture#map(java.util.function.Function)} or 
   * {@link ListenableFuture#flatMap(java.util.function.Function)} should be counted towards the 
   * concurrency limit.  Specifying {@code false} will release the limit as soon as the original 
   * task completes.  Specifying {@code true} will continue to enforce the limit until all listeners 
   * (without an executor) complete.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of tasks to run in parallel
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   * @param sorter Implementation of {@link Comparator} to sort the task queue being limited
   */
  @SuppressWarnings("unchecked")
  public OrderedExecutorLimiter(Executor executor, int maxConcurrency, 
                                boolean limitFutureListenersExecution,
                                final Comparator<? super T> sorter) {
    ArgumentVerifier.assertNotNull(sorter, "sorter");
    
    limiter = new ExecutorLimiter(executor, maxConcurrency, limitFutureListenersExecution, 
                                  new PriorityBlockingQueue<>(INITIAL_QUEUE_SIZE, (rc1, rc2) -> 
                                          sorter.compare((T)rc1.getContainedRunnable(), 
                                                         (T)rc2.getContainedRunnable())));
  }
  
  /**
   * The same as {@link org.threadly.concurrent.SubmitterExecutor#submit(Runnable)} but 
   * typed for the sortable task.
   * 
   * @param task runnable to be executed
   * @return a future to know when the task has completed
   */
  public ListenableFuture<?> submit(T task) {
    return submit(task, null);
  }
  
  /**
   * The same as {@link org.threadly.concurrent.SubmitterExecutor#submit(Runnable, Object)} but 
   * typed for the sortable task.
   * 
   * @param <R> type of result for future
   * @param task runnable to be executed
   * @param result result to be returned from resulting future .get() when runnable completes
   * @return a future to know when the task has completed
   */
  public <R> ListenableFuture<R> submit(T task, R result) {
    return limiter.submit(task, result);
  }
  
  /**
   * The same as {@link Executor#execute(Runnable)} but typed for the sortable task.
   * 
   * @param task runnable to be executed
   */
  public void execute(T task) {
    limiter.execute(task);
  }
}

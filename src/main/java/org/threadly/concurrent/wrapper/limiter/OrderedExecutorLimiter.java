package org.threadly.concurrent.wrapper.limiter;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;

import org.threadly.concurrent.ContainerHelper;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
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
  public OrderedExecutorLimiter(Executor executor, int maxConcurrency, Comparator<? super T> sorter) {
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
                                  new PriorityBlockingQueue<>(INITIAL_QUEUE_SIZE, (rc1, rc2) -> {
                                    T r1 = runnableTypeFromContainer(rc1);
                                    T r2 = runnableTypeFromContainer(rc2);
                                    return sorter.compare(r1, r2);
                                  })) {
      @Override
      protected <FT> ListenableFutureTask<FT> makeListenableFutureTask(Callable<FT> task) {
        // fix possible NPE on sorting by callable if the future completes (cancel) while still in queue
        return new OrderedListenableFutureTask<>(limiter.waitingTasks, task, this);
      }
      
      @Override
      protected boolean taskCapacity() {
        checkTaskCapacity();
        return super.taskCapacity();
      }
    };
  }
  
  @SuppressWarnings("unchecked")
  private T runnableTypeFromContainer(RunnableContainer rc) {
    Runnable r = rc.getContainedRunnable();
    if (r instanceof OrderedListenableFutureTask) {
      Callable<?> c = ((ListenableFutureTask<?>)r).getContainedCallable();
      if (c instanceof RunnableCallableAdapter) {
        r = ((RunnableCallableAdapter<?>)c).getContainedRunnable();
      } else {
        throw new IllegalStateException("Unexpected callable type: " + 
                                          (c == null ? "null" : c.getClass().toString()));
      }
    }
    return (T)r;
  }
  
  /**
   * Call to check what the maximum concurrency this limiter will allow.
   * <p>
   * See {@link ExecutorLimiter#getMaxConcurrency()}.
   * 
   * @since 6.3
   * @return maximum concurrent tasks to be run
   */
  public int getMaxConcurrency() {
    return limiter.getMaxConcurrency();
  }
  
  /**
   * Updates the concurrency limit for this limiter.
   * <p>
   * See {@link ExecutorLimiter#setMaxConcurrency(int)}.
   * 
   * @since 6.3
   * @param maxConcurrency maximum quantity of tasks to run in parallel
   */
  public void setMaxConcurrency(int maxConcurrency) {
    limiter.setMaxConcurrency(maxConcurrency);
  }
  
  /**
   * Query how many tasks are being withheld from the parent scheduler.
   * <p>
   * See {@link ExecutorLimiter#getUnsubmittedTaskCount()}.
   * 
   * @since 6.3
   * @return Quantity of tasks queued in this limiter
   */
  public int getUnsubmittedTaskCount() {
    return limiter.getUnsubmittedTaskCount();
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
  
  /**
   * A protected function to provide overriding capabilities similar to 
   * {@link ExecutorLimiter#taskCapacity()}.  This is invoked before the limiter is checked if a 
   * task can be executed, providing an opportunity to adjust the limits before they are checked.
   * 
   * @since 6.3
   */
  protected void checkTaskCapacity() {
    // ignored by default
  }
  
  /**
   * Implementation of {@link ListenableFutureTask} which can still provide the contained 
   * {@link Callable} after being canceled.  This is necessary so that the 
   * {@link OrderedExecutorLimiter} can still sort the canceled task.
   * 
   * @param <FT> Type of result provided from the future.
   */
  protected static class OrderedListenableFutureTask<FT> extends ListenableFutureTask<FT> {
    private Collection<? extends RunnableContainer> taskQueue;
    private Callable<FT> task;
    
    public OrderedListenableFutureTask(Collection<? extends RunnableContainer> taskQueue, 
                                       Callable<FT> task, Executor executingExecutor) {
      super(task, executingExecutor);
      
      this.taskQueue = taskQueue;
      this.task = task;
    }

    @Override
    public Callable<FT> getContainedCallable() {
      return task;
    }
    
    @Override
    protected void handleCompleteState() {
      try {
        if (isCancelled()) {
          // ensure this is removed from the queue before we can clear the task reference
          ContainerHelper.remove(taskQueue, OrderedListenableFutureTask.this);
        }
      } finally {
        this.taskQueue = null;
        this.task = null;
        
        super.handleCompleteState();
      }
    }
  }
}

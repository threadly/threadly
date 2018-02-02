package org.threadly.concurrent.wrapper.limiter;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * This class is designed to limit how much parallel execution happens on a provided 
 * {@link Executor}.  This allows the user to have one thread pool for all their code, and if they 
 * want certain sections to have less levels of parallelism (possibly because those those sections 
 * would completely consume the global pool), they can wrap the executor in this class.
 * <p>
 * Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.
 * <p>
 * This is an alternative from having to create multiple thread pools.  By using this you also 
 * are able to accomplish more efficiently thread use than multiple thread pools would.
 * <p>
 * If limiting to a single thread, please see {@link SingleThreadSchedulerSubPool} as a possible 
 * alternative.
 * 
 * @since 4.6.0 (since 1.0.0 at org.threadly.concurrent.limiter)
 */
public class ExecutorLimiter implements SubmitterExecutor {
  protected static final boolean DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION = true;
  
  protected final Executor executor;
  protected final Queue<RunnableRunnableContainer> waitingTasks;
  protected final boolean limitFutureListenersExecution;
  private final AtomicInteger currentlyRunning;
  private volatile int maxConcurrency;
  
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of tasks to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    this(executor, maxConcurrency, DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }
  
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
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
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency, 
                         boolean limitFutureListenersExecution) {
    ArgumentVerifier.assertNotNull(executor, "executor");
    ArgumentVerifier.assertGreaterThanZero(maxConcurrency, "maxConcurrency");

    this.executor = executor;
    this.waitingTasks = new ConcurrentLinkedQueue<>();
    this.limitFutureListenersExecution = limitFutureListenersExecution;
    this.currentlyRunning = new AtomicInteger(0);
    this.maxConcurrency = maxConcurrency;
  }
  
  @Override
  public void execute(Runnable task) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    executeOrQueue(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return submit(new RunnableCallableAdapter<>(task, result));
  }
  
  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<>(false, task, this);
    
    executeOrQueue(lft, lft);
    
    return lft;
  }
  
  /**
   * Call to check what the maximum concurrency this limiter will allow.
   * 
   * @return maximum concurrent tasks to be run
   */
  public int getMaxConcurrency() {
    return maxConcurrency;
  }
  
  /**
   * Updates the concurrency limit for this wrapper.  If reducing the the limit, there will be no 
   * attempt or impact on tasks already limiting.  Instead new tasks just wont be submitted to the 
   * parent pool until existing tasks complete and go below the new limit.
   * 
   * @since 5.4
   * @param maxConcurrency maximum quantity of tasks to run in parallel
   */
  public void setMaxConcurrency(int maxConcurrency) {
    ArgumentVerifier.assertGreaterThanZero(maxConcurrency, "maxConcurrency");
    
    boolean increasing = this.maxConcurrency < maxConcurrency; 
    this.maxConcurrency = maxConcurrency;
    if (increasing) {
      consumeAvailable();
    }
  }
  
  /**
   * Returns how many tasks are currently being "limited" and thus are in queue to run from this 
   * limiter.
   * 
   * @return Quantity of tasks queued in this limiter
   */
  public int getUnsubmittedTaskCount() {
    return waitingTasks.size();
  }
  
  /**
   * Thread safe verification that the pool has space remaining to accept additional tasks.
   * <p>
   * If this returns {@code true} {@code currentlyRunning} has been incremented and it expects the 
   * task to run will invoke {@link #handleTaskFinished()} when completed.
   * 
   * @return {@code true} if the task can be submitted to the pool
   */
  protected boolean canSubmitTaskToPool() {
    while (true) {  // loop till we have a result
      int currentValue = currentlyRunning.get();
      if (currentValue < maxConcurrency) {
        if (currentlyRunning.compareAndSet(currentValue, currentValue + 1)) {
          return true;
        } // else retry in while loop
      } else {
        return false;
      }
    }
  }
  
  /**
   * Submit any tasks that we can to the parent executor (dependent on our pools limit).
   */
  protected void consumeAvailable() {
    if (currentlyRunning.get() >= maxConcurrency || waitingTasks.isEmpty()) {
      // shortcut before we lock
      return;
    }
    /* must synchronize in queue consumer to avoid multiple threads from consuming tasks in 
     * parallel and possibly emptying after .isEmpty() check but before .poll()
     */
    synchronized (this) {
      while (! waitingTasks.isEmpty() && canSubmitTaskToPool()) {
        // by entering loop we can now execute task
        executor.execute(waitingTasks.poll());
      }
    }
  }
  
  /**
   * Check that not only are we able to submit tasks to the pool, but there are no tasks currently 
   * waiting to already be submitted.  If only {@link #canSubmitTaskToPool()} is checked, tasks 
   * may be able to cut in line with tasks that are already queued in the waiting queue.
   * 
   * @return true if the task can be submitted to the pool 
   */
  protected boolean canRunTask() {
    return waitingTasks.isEmpty() && canSubmitTaskToPool();
  }
  
  /**
   * Called to indicate that hold for the task execution should be released. 
   */
  private void releaseExecutionLimit() {
    currentlyRunning.decrementAndGet();
    
    consumeAvailable(); // allow any waiting tasks to run
  }

  /**
   * This is called once a task is ready to be executed (or if unable to execute immediately, 
   * queued).  In addition to the task itself, this function takes in any future which represents 
   * task execution (if available, otherwise {@code null}).  Passing in as a separate argument 
   * allows us to avoid a {@code instanceof} check, but does require it to be specified for 
   * pre-future listener completion support.
   * 
   * @param task Task to be executed
   * @param future Future to represent task completion or {@code null} if not available
   */
  protected void executeOrQueue(Runnable task, ListenableFuture<?> future) {
    if (limitFutureListenersExecution || future == null) {
      executeOrQueueWrapper(new LimiterRunnableWrapper(task));
    } else {
      // we will release the limit restriction as soon as the future completes.
      // listeners should be invoked in order, so we just need to be the first listener here
      // We add a `SameThreadSubmitterExecutor` so that we get executed first as if it was async
      future.addListener(this::releaseExecutionLimit, SameThreadSubmitterExecutor.instance());

      if (canRunTask()) {
        executor.execute(task);
      } else {
        addToQueue(new TransparentRunnableContainer(task));
      }
    }
  }
  
  /**
   * Executes the wrapper if there is room in the limiter, otherwise it will queue the wrapper to 
   * be executed once the limiter has room.
   * 
   * @param lrw Wrapper that is ready to execute once there is available slots in the limiter
   */
  protected void executeOrQueueWrapper(LimiterRunnableWrapper lrw) {
    if (canRunTask()) {
      executor.execute(lrw);
    } else {
      addToQueue(lrw);
    }
  }
  
  /**
   * Adds the wrapper to the queue.  After adding to the queue it will be verified that there is 
   * not available slots in the limiter to consume (it or others) from the queue.
   * <p>
   * It is expected that you already attempted to run the task (by calling {@link #canRunTask()} 
   * before deferring to this.
   * 
   * @param lrw {@link LimiterRunnableWrapper} to add to the queue
   */
  protected void addToQueue(RunnableRunnableContainer lrw) {
    waitingTasks.add(lrw);
    consumeAvailable(); // call to consume in case task finished after first check
  }

  // TODO - make higher level and reuse for this common interface combination?  Or avoid javadocs pollution?
  /**
   * Simple combination of {@link RunnableContainer} and {@link Runnable}.  This allows us to 
   * specify two possible run implementations while have a collection of {@link RunnableContainer}'s 
   * so that we can do task removal easily.
   * 
   * @since 5.7
   */
  protected interface RunnableRunnableContainer extends RunnableContainer, Runnable {
    // intentionally left blank
  }
  
  /**
   * Generic wrapper for runnables which are used within the limiters.  This wrapper ensures that 
   * {@link #handleTaskFinished()} will be called after the task completes.
   * 
   * @since 1.0.0
   */
  protected class LimiterRunnableWrapper implements RunnableRunnableContainer {
    protected final Runnable runnable;
    
    public LimiterRunnableWrapper(Runnable runnable) {
      this.runnable = runnable;
    }
    
    /**
     * Called immediately after contained task finishes.  That way any additional cleanup needed 
     * can be run.
     */
    protected void doAfterRunTasks() {
      // nothing in the default implementation
    }
    
    @Override
    public void run() {
      try {
        runnable.run();
      } finally {
        try {
          doAfterRunTasks();
        } finally {
          releaseExecutionLimit();
        }
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return runnable;
    }
  }
  
  /**
   * An implementation of {@link RunnableRunnableContainer} where the {@link Runnable#run()} 
   * implementation delegates purely to the contained runnable.
   * 
   * @since 5.7
   */
  protected static class TransparentRunnableContainer implements RunnableRunnableContainer {
    protected final Runnable task;
    
    protected TransparentRunnableContainer(Runnable task) {
      this.task = task;
    }
    
    @Override
    public void run() {
      task.run();
    }
    
    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
}

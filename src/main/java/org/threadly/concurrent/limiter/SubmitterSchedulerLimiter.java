package org.threadly.concurrent.limiter;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import org.threadly.concurrent.SubmitterSchedulerInterface;
import org.threadly.concurrent.VirtualRunnable;
import org.threadly.concurrent.future.FutureFuture;

/**
 * This class is designed to limit how much parallel execution happens 
 * on a provided {@link SubmitterSchedulerInterface}.  This allows the 
 * implementor to have one thread pool for all their code, and if 
 * they want certain sections to have less levels of parallelism 
 * (possibly because those those sections would completely consume the 
 * global pool), they can wrap the executor in this class.
 * 
 * Thus providing you better control on the absolute thread count and 
 * how much parallelism can occur in different sections of the program.  
 * 
 * Thus avoiding from having to create multiple thread pools, and also 
 * using threads more efficiently than multiple thread pools would.
 * 
 * @author jent - Mike Jensen
 */
public class SubmitterSchedulerLimiter extends AbstractSchedulerLimiter 
                                       implements SubmitterSchedulerInterface {
  protected final SubmitterSchedulerInterface scheduler;
  protected final Queue<Wrapper> waitingTasks;
  
  /**
   * Constructs a new limiter that implements the {@link SubmitterSchedulerInterface}.
   * 
   * @param scheduler {@link SubmitterSchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public SubmitterSchedulerLimiter(SubmitterSchedulerInterface scheduler, 
                                int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SubmitterSchedulerInterface}.
   * 
   * @param scheduler {@link SubmitterSchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool (null to not change thread names)
   */
  public SubmitterSchedulerLimiter(SubmitterSchedulerInterface scheduler, 
                                int maxConcurrency, String subPoolName) {
    super(maxConcurrency, subPoolName);
    
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler");
    }
    
    this.scheduler = scheduler;
    waitingTasks = new ConcurrentLinkedQueue<Wrapper>();
  }
  
  @Override
  protected void consumeAvailable() {
    /* must synchronize in queue consumer to avoid 
     * multiple threads from consuming tasks in parallel 
     * and possibly emptying after .isEmpty() check but 
     * before .poll()
     */
    synchronized (this) {
      while (! waitingTasks.isEmpty() && canRunTask()) {
        // by entering loop we can now execute task
        Wrapper next = waitingTasks.poll();
        if (next.hasFuture()) {
          if (next.isCallable()) {
            Future<?> f = scheduler.submit(next.getCallable());
            next.getFuture().setParentFuture(f);
          } else {
            Future<?> f = scheduler.submit(next.getRunnable());
            next.getFuture().setParentFuture(f);
          }
        } else {
          // all callables will have futures, so we know this is a runnable
          scheduler.execute(next.getRunnable());
        }
      }
    }
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public void execute(Runnable task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    RunnableFutureWrapper wrapper = new RunnableFutureWrapper(task, null);
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      scheduler.execute(wrapper);
    } else {
      waitingTasks.add(wrapper);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    FutureFuture<T> ff = new FutureFuture<T>();
    
    doSubmit(task, result, ff);
    
    return ff;
  }
  
  private <T> void doSubmit(Runnable task, T result, 
                            FutureFuture<T> ff) {
    RunnableFutureWrapper wrapper = new RunnableFutureWrapper(task, ff);
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      ff.setParentFuture(scheduler.submit(wrapper, result));
    } else {
      waitingTasks.add(wrapper);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    FutureFuture<T> ff = new FutureFuture<T>();
    
    doSubmit(task, ff);
    
    return ff;
  }
  
  private <T> void doSubmit(Callable<T> task, 
                            FutureFuture<T> ff) {
    CallableFutureWrapper<T> wrapper = new CallableFutureWrapper<T>(task, ff);
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      ff.setParentFuture(scheduler.submit(wrapper));
    } else {
      waitingTasks.add(wrapper);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    if (delayInMs == 0) {
      execute(task);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<Object>(task, null, null), 
                         delayInMs);
    }
  }

  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> Future<T> submitScheduled(Runnable task, T result, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }

    FutureFuture<T> ff = new FutureFuture<T>();
    if (delayInMs == 0) {
      doSubmit(task, result, ff);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<T>(task, result, ff), 
                         delayInMs);
    }
    
    return ff;
  }

  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }

    FutureFuture<T> ff = new FutureFuture<T>();
    if (delayInMs == 0) {
      doSubmit(task, ff);
    } else {
      scheduler.schedule(new DelayedExecutionCallable<T>(task, ff), 
                         delayInMs);
    }
    
    return ff;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    
    RecurringRunnableWrapper rrw = new RecurringRunnableWrapper(task, recurringDelay);
    
    if (initialDelay == 0) {
      execute(rrw);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<Object>(rrw, null, null), 
                         initialDelay);
    }
  }
  
  /**
   * Small runnable that allows scheduled tasks to pass through 
   * the same execution queue that immediate execution has to.
   * 
   * @author jent - Mike Jensen
   */
  protected class DelayedExecutionRunnable<T> extends VirtualRunnable {
    private final Runnable runnable;
    private final T runnableResult;
    private final FutureFuture<T> future;

    public DelayedExecutionRunnable(Runnable runnable, T runnableResult, 
                                    FutureFuture<T> future) {
      this.runnable = runnable;
      this.runnableResult = runnableResult;
      this.future = future;
    }
    
    @Override
    public void run() {
      if (future == null) {
        execute(runnable);
      } else {
        doSubmit(runnable, runnableResult, future);
      }
    }
  }
  
  /**
   * Small runnable that allows scheduled tasks to pass through 
   * the same execution queue that immediate execution has to.
   * 
   * @author jent - Mike Jensen
   */
  protected class DelayedExecutionCallable<T> extends VirtualRunnable {
    private final Callable<T> callable;
    private final FutureFuture<T> future;

    public DelayedExecutionCallable(Callable<T> runnable, 
                                    FutureFuture<T> future) {
      this.callable = runnable;
      this.future = future;
    }
    
    @Override
    public void run() {
      doSubmit(callable, future);
    }
  }

  /**
   * Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   */
  protected class RecurringRunnableWrapper extends LimiterRunnableWrapper
                                           implements Wrapper  {
    private final long recurringDelay;
    private final DelayedExecutionRunnable<?> delayRunnable;
    
    public RecurringRunnableWrapper(Runnable runnable, 
                                    long recurringDelay) {
      super(runnable);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new DelayedExecutionRunnable<Object>(this, null, null);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay);
    }

    @Override
    public boolean isCallable() {
      return false;
    }

    @Override
    public FutureFuture<?> getFuture() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFuture() {
      return false;
    }

    @Override
    public Callable<?> getCallable() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Runnable getRunnable() {
      return this;
    }
  }

  /**
   * Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   */
  protected class RunnableFutureWrapper extends LimiterRunnableWrapper
                                        implements Wrapper  {
    private final FutureFuture<?> future;
    
    public RunnableFutureWrapper(Runnable runnable, 
                                 FutureFuture<?> future) {
      super(runnable);
      
      this.future = future;
    }
    
    @Override
    protected void doAfterRunTasks() {
      // nothing to do here
    }

    @Override
    public boolean isCallable() {
      return false;
    }

    @Override
    public FutureFuture<?> getFuture() {
      return future;
    }

    @Override
    public boolean hasFuture() {
      return future != null;
    }

    @Override
    public Callable<?> getCallable() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Runnable getRunnable() {
      return this;
    }
  }

  /**
   * Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   * @param <T> type for return of callable contained within wrapper
   */
  protected class CallableFutureWrapper<T> extends LimiterCallableWrapper<T>
                                           implements Wrapper {
    private final FutureFuture<?> future;
    
    public CallableFutureWrapper(Callable<T> callable, 
                                 FutureFuture<?> future) {
      super(callable);
      
      this.future = future;
    }

    @Override
    public boolean isCallable() {
      return true;
    }

    @Override
    public FutureFuture<?> getFuture() {
      return future;
    }

    @Override
    public boolean hasFuture() {
      return future != null;
    }

    @Override
    public Callable<?> getCallable() {
      return this;
    }

    @Override
    public Runnable getRunnable() {
      throw new UnsupportedOperationException();
    }
  }
  
  /**
   * Interface so that we can handle both callables and runnables.
   * 
   * @author jent - Mike Jensen
   */
  private interface Wrapper {
    public boolean isCallable();
    public FutureFuture<?> getFuture();
    public boolean hasFuture();
    public Callable<?> getCallable();
    public Runnable getRunnable();
  }
}

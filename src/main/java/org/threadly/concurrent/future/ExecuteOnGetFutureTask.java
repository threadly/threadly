package org.threadly.concurrent.future;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>This future task has a special ability to start execution in the thread requesting the 
 * {@link #get()} IF it has not already started.  It is still expected that this future will be 
 * provided to an executor, but if that executor has been unable to start execution by the time 
 * {@link #get()} is requested, execution will start on the {@link #get()} thread.</p>
 * 
 * <p>This can make sense if for example you need to have the result of a given future before you 
 * can go forward with additional work.  That way if the thread pool is too busy with other 
 * things, we can try to get the result for this work unit as fast as possible (once we are ready 
 * for it).  If execution starts on the {@link #get()} call, once the task is started in the thread 
 * pool, it will return immediately (WITHOUT invoking the task contained in the future).</p>
 * 
 * <p>This does come with some details to be aware of.  Execution may only occur on the 
 * {@link #get()} request.  Requesting {@link #get(long, java.util.concurrent.TimeUnit)} will NOT 
 * cause execution to start (as we can not ensure a timeout will be respected).  In addition this 
 * can not work with recurring tasks.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.4.0
 * @param <T> type of future implementation
 */
public class ExecuteOnGetFutureTask<T> extends ListenableFutureTask<T> {
  private final AtomicBoolean executionStarted = new AtomicBoolean(false);
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   */
  public ExecuteOnGetFutureTask(Runnable task) {
    super(false, task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public ExecuteOnGetFutureTask(Runnable task, T result) {
    super(false, task, result);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param task callable to be run
   */
  public ExecuteOnGetFutureTask(Callable<T> task) {
    super(false, task);
  }
  
  /**
   * Starts execution of task if it has not already started.  This is thread safe, and will ensure 
   * that execution will only occur once.
   */
  protected void executeIfNotStarted() {
    if (executionStarted.compareAndSet(false, true)) {
      super.run();
    }
  }
  
  @Override
  public void run() {
    executeIfNotStarted();
  }
  
  @Override
  public T get() throws InterruptedException, ExecutionException {
    executeIfNotStarted();
    
    return super.get();
  }
}

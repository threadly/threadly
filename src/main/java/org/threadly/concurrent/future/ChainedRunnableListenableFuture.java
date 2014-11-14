package org.threadly.concurrent.future;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>This class makes it easier to chain together work units that need to return a future.  It 
 * ensures that execution happens on an {@link Executor}, but allows you to gain performance 
 * improvements by reducing thread thrashing.</p>
 * 
 * <p>The way to use this is to construct this class to wrap a {@link Callable}.  Then add this 
 * class as a listener using {@link ListenableFuture#addListener(Runnable)} (NOT the one that 
 * accepts an executor).  When this class executes (from when the future completes), it ensures 
 * that it is running on the thread pool that the original future completed on.  If it is not 
 * (because the Future completed before the  {@link ListenableFuture#addListener(Runnable)} call) 
 * then it will submit for execution on the {@link Executor} that was provided at construction 
 * time.</p>
 * 
 * <p>This instance can then be returned as the {@link ListenableFuture}.</p>
 * 
 * <p>For chaining work where you know you will need the result from one future before the result 
 * of the last one, you can get substantial performance improvements by reducing the load on the 
 * thread pool.  Instead of releasing the thread back to the pool, after execution completed on 
 * the first future, it immediately starts execution on this.  So this only makes sense where 
 * execution on the pool is likely longer running than the submission rate.</p>
 * 
 * <p>Some important notes to keep in mind with this...</p>
 * <ul>
 * <li>If you chain too many listeners together, eventually a stack overflow is possible.
 * <li>Because futures may live longer than previously, you may want to consider passing in a 
 * {@link ClearOnGetSettableListenableFuture} into the constructor 
 * {@link #ChainedRunnableListenableFuture(Thread, SettableListenableFuture, Executor, Callable)}.
 * <li>This only makes sense in some specific conditions, specifically with high quantity of tasks 
 * which run for more than a few hundred milliseconds.
 * </ul>
 * 
 * @author jent - Mike Jensen
 * @param <T> The result object type returned by this future
 */
public class ChainedRunnableListenableFuture<T> extends AbstractNoncancelableListenableFuture<T> 
                                                implements ListenableRunnableFuture<T> {
  protected final Thread creationThread;
  protected final Executor executor;
  protected final SettableListenableFuture<T> settableFuture;
  private Callable<? extends T> callable;
  private boolean rescheduled;
  
  /**
   * Constructs a new {@link ChainedRunnableListenableFuture}.
   * 
   * @param executor Executor to submit execution on if execution starts on the same thread constructed
   * @param callable Callable to invoke for result to satisfy the future
   */
  public ChainedRunnableListenableFuture(Executor executor, Callable<? extends T> callable) {
    this(Thread.currentThread(), new SettableListenableFuture<T>(), executor, callable);
  }
  
  /**
   * Constructs a new {@link ChainedRunnableListenableFuture}.  
   * 
   * This constructor lets you control the backing {@link SettableListenableFuture} 
   * implementation.  The most common use case of this would be to substitute a 
   * {@link ClearOnGetSettableListenableFuture} to avoid possibly increasing memory load from 
   * using this.  If using that future be sure to understand the restrictions of only being able 
   * to query for the result once.
   * 
   * @param backingFuture Future to use internally to maintain the result for the future
   * @param executor Executor to submit execution on if execution starts on the same thread constructed
   * @param callable Callable to invoke for result to satisfy the future
   */
  public ChainedRunnableListenableFuture(SettableListenableFuture<T> backingFuture, 
                                         Executor executor, Callable<? extends T> callable) {
    this(Thread.currentThread(), backingFuture, executor, callable);
  }
  
  /**
   * Constructs a new {@link ChainedRunnableListenableFuture}.  
   * 
   * This constructor allows you to control what Thread it should use to consider it's "creation" 
   * thread.  If execution starts on this thread, the task will be re-submitted to the provided 
   * {@link Executor}.
   * 
   * @param creationThread Reference thread for execution, if execution happens on this thread it will be re-executed
   * @param executor Executor to submit execution on if execution starts on the same thread constructed
   * @param callable Callable to invoke for result to satisfy the future
   */
  public ChainedRunnableListenableFuture(Thread creationThread, Executor executor, Callable<? extends T> callable) {
    this(creationThread, new SettableListenableFuture<T>(), executor, callable);
  }
  
  /**
   * Constructs a new {@link ChainedRunnableListenableFuture}.  
   * 
   * This constructor lets you control the backing {@link SettableListenableFuture} 
   * implementation.  The most common use case of this would be to substitute a 
   * {@link ClearOnGetSettableListenableFuture} to avoid possibly increasing memory load from 
   * using this.  If using that future be sure to understand the restrictions of only being able 
   * to query for the result once.  
   * 
   * This constructor also allows you to control what Thread it should use to consider it's 
   * "creation" thread.  If execution starts on this thread, the task will be re-submitted to the 
   * provided {@link Executor}.
   * 
   * @param creationThread Reference thread for execution, if execution happens on this thread it will be re-executed
   * @param backingFuture Future to use internally to maintain the result for the future
   * @param executor Executor to submit execution on if execution starts on the same thread constructed
   * @param callable Callable to invoke for result to satisfy the future
   */
  public ChainedRunnableListenableFuture(Thread creationThread, SettableListenableFuture<T> backingFuture, 
                                         Executor executor, Callable<? extends T> callable) {
    ArgumentVerifier.assertNotNull(creationThread, "creationThread");
    ArgumentVerifier.assertNotNull(backingFuture, "backingFuture");
    ArgumentVerifier.assertNotNull(executor, "executor");
    ArgumentVerifier.assertNotNull(callable, "callable");
    
    this.creationThread = creationThread;
    this.executor = executor;
    this.settableFuture = backingFuture;
    this.callable =  callable;
    this.rescheduled = false;
  }

  @Override
  public void run() {
    if (! rescheduled && Thread.currentThread() == creationThread) {
      rescheduled = true;
      executor.execute(this);
    } else if (settableFuture.isDone()) {
      throw new IllegalStateException("Can not be executed multiple times");
    } else {
      try {
        T result = callable.call();
        settableFuture.setResult(result);
      } catch (Throwable t) {
        settableFuture.setFailure(t);
      } finally {
        // set callable to null to allow collection now that we are done
        callable = null;
      }
    }
  }

  @Override
  public void addListener(Runnable listener) {
    settableFuture.addListener(listener);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    settableFuture.addListener(listener, executor);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback) {
    settableFuture.addCallback(callback);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor) {
    settableFuture.addCallback(callback, executor);
  }

  @Override
  public boolean isDone() {
    return settableFuture.isDone();
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return settableFuture.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, 
                                                   TimeoutException {
    return settableFuture.get(timeout, unit);
  }
}

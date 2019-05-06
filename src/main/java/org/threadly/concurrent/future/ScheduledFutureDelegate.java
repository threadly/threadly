package org.threadly.concurrent.future;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Implementation of the {@link ListenableScheduledFuture} interface.  This design delegates 
 * between a {@link Delayed} instance and {@link ListenableFuture} instance.
 * 
 * @deprecated To be removed without public replacement, if used please open github issue 
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
@Deprecated
public class ScheduledFutureDelegate<T> implements ListenableScheduledFuture<T> {
  protected final ListenableFuture<? extends T> futureImp;
  protected final Delayed delayed;
  
  /**
   * Constructs a new {@link ScheduledFutureDelegate} with the provided instances to call to for 
   * each interface.
   * 
   * @param futureImp implementation to call to for all Future calls
   * @param delayed implementation to call to for getDelay and compareTo
   */
  public ScheduledFutureDelegate(ListenableFuture<? extends T> futureImp, Delayed delayed) {
    this.futureImp = futureImp;
    this.delayed = delayed;
  }
  
  @Override
  public long getDelay(TimeUnit unit) {
    return delayed.getDelay(unit);
  }

  @Override
  public int compareTo(Delayed o) {
    return delayed.compareTo(o);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return futureImp.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return futureImp.isCancelled();
  }

  @Override
  public boolean isDone() {
    return futureImp.isDone();
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return futureImp.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                                   ExecutionException, TimeoutException {
    return futureImp.get(timeout, unit);
  }

  @Override
  public ListenableFuture<T> listener(Runnable listener, Executor executor, 
                                      ListenerOptimizationStrategy optimizeExecution) {
    futureImp.listener(listener, executor, optimizeExecution);
    
    return this;
  }

  @Override
  public ListenableFuture<T> callback(FutureCallback<? super T> callback, Executor executor, 
                                      ListenerOptimizationStrategy optimizeExecution) {
    futureImp.callback(callback, executor, optimizeExecution);
    
    return this;
  }

  @Override
  public ListenableFuture<T> resultCallback(Consumer<? super T> callback, Executor executor, 
                                            ListenerOptimizationStrategy optimizeExecution) {
    futureImp.resultCallback(callback, executor, optimizeExecution);
    
    return this;
  }

  @Override
  public ListenableFuture<T> failureCallback(Consumer<Throwable> callback, Executor executor, 
                                             ListenerOptimizationStrategy optimizeExecution) {
    futureImp.failureCallback(callback, executor, optimizeExecution);
    
    return this;
  }

  @Override
  public StackTraceElement[] getRunningStackTrace() {
    return futureImp.getRunningStackTrace();
  }
}

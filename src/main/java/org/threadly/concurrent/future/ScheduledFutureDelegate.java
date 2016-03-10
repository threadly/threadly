package org.threadly.concurrent.future;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>Implementation of the {@link ListenableScheduledFuture} interface.  This design delegates 
 * between a {@link Delayed} instance and {@link ListenableFuture} instance..</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
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
  public void addListener(Runnable listener) {
    futureImp.addListener(listener);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    futureImp.addListener(listener, executor);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback) {
    futureImp.addCallback(callback);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor) {
    futureImp.addCallback(callback, executor);
  }
}

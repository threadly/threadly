package org.threadly.concurrent.future;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation which handles the {@link ScheduledFuture} interface as well 
 * as the {@link ListenableFuture} interface.  It does so by defaulting to 
 * two implementations.  A delayed object for the {@link ScheduledFuture} interface 
 * and a implementation of the {@link ListenableFuture}.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of result for future
 */
public class ListenableScheduledFuture<T> implements ScheduledFuture<T>, ListenableFuture<T> {
  protected final ListenableFuture<T> futureImp;
  protected final Delayed delayed;
  
  /**
   * Constructs a new {@link ListenableScheduledFuture} with the provided 
   * Implementations to call to.
   * 
   * @param futureImp implementation to call to for all Future calls
   * @param delayed implementation to call to for getDelay and compareTo
   */
  public ListenableScheduledFuture(ListenableFuture<T> futureImp, 
                                   Delayed delayed) {
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
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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
}

package org.threadly.concurrent.future;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.util.SuppressedStackRuntimeException;

/**
 * Adapter from java's {@link Future} to threadly's {@link ListenableFuture}.  This transfers the 
 * state (ie result, failure, or canceled) from the source future.  In order for this translation 
 * to occur a thread is needed to notify potential listeners when the future completes.  When 
 * {@link ListenableFutureTask#run()} is invoked we will block (if delegate future is not complete) 
 * waiting for the result from the provided future.  Completing with the same state once the 
 * provided future completes.
 * <p>
 * The most common way to use this class is to construct it with the future to adapt from, then 
 * provide this future to an executor to block waiting for the provided future to complete.  If 
 * this is used for high quantities keep in mind a lot of the threads will idly be waiting for 
 * results, typically a native threadly pool may be a better option.
 * 
 * @param <T> Type of result returned from the future
 * @since 4.9.0
 */
public class ListenableFutureAdapterTask<T> extends ListenableFutureTask<T> {
  protected final Future<? extends T> f;
  
  /**
   * Constructs a new {@link ListenableFutureAdapterTask}.
   * 
   * @param f Future to get result and source final state from
   */
  public ListenableFutureAdapterTask(final Future<? extends T> f) {
    super(false, new Callable<T>() {
      @Override
      public T call() throws Exception {
        try {
          return f.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof Exception) {
            throw (Exception)cause;
          } else {
            throw new SuppressedStackRuntimeException(cause);
          }
        }
      }
    });
    
    this.f = f;
  }
  
  @Override
  public void run() {
    // must check if canceled before we run, otherwise we can't transition the state
    if (f.isCancelled()) {
      super.cancel(false);
    }
    super.run();
  }
}

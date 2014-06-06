package org.threadly.concurrent.future;

/**
 * <p>Abstract class for futures that can't be canceled.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.1.0
 * @param <T> type of object returned by the future
 */
abstract class AbstractNoncancelableListenableFuture<T> implements ListenableFuture<T> {
  /**
   * This has no effect in this implementation, as this future can not be canceled.
   * 
   * @param  mayInterruptIfRunning will be ignored
   * @return will always return false, as this future can't be canceled
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }
}

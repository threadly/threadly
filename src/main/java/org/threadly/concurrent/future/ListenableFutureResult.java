package org.threadly.concurrent.future;

/**
 * <p>This class is designed to be a helper when returning a single 
 * result asynchronously.  This is particularly useful if this result 
 * is produced over multiple threads (and thus the scheduler returned 
 * future is not useful).</p>
 * 
 * @deprecated Please use SettableListenableFuture, this will be removed in 2.0.0
 * 
 * @author jent - Mike Jensen
 * @since 1.1.0
 * @param <T> type of returned object
 */
@Deprecated
public class ListenableFutureResult<T> extends SettableListenableFuture<T> {
  /**
   * Constructs a new {@link ListenableFutureResult}.  You can return this 
   * immediately and provide a result to the object later when it is ready.
   */
  public ListenableFutureResult() {
    // nothing added here
  }
}

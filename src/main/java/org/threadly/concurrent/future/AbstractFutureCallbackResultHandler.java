package org.threadly.concurrent.future;

/**
 * An abstract implementation of {@link FutureCallback} where failures are ignored.  This can 
 * allow you to easily do an action only when a result is produced condition (with the result 
 * provided by {@link FutureCallback#handleResult(Object)}.
 * <p>
 * It is important to know that if a failure did occur, and your using this, unless your using 
 * some other means to detect it (like {@link ListenableFuture#listener(Runnable)}, you may never 
 * know the computation is complete.
 * <p>
 * For a simpler construction using a lambda look at {@link FutureCallbackResultHandler}.
 * 
 * @deprecated Instead use a {@link java.util.function.Consumer} provided to 
 *               {@link ListenableFuture#resultCallback(java.util.function.Consumer)}
 * 
 * @param <T> Type of result returned
 * @since 4.4.0
 */
@Deprecated
public abstract class AbstractFutureCallbackResultHandler<T> implements FutureCallback<T> {
  @Override
  public void handleFailure(Throwable t) {
    // ignored
  }
}

package org.threadly.concurrent.future;

/**
 * An abstract implementation of {@link FutureCallback} where the result is ignored.  This can 
 * allow you to easily do an action only on a failure condition (with the failure provided by 
 * {@link FutureCallback#handleFailure(Throwable)}).
 * <p>
 * For simpler construction using a lambda look at {@link FutureCallbackFailureHandler}.
 * 
 * @deprecated Instead use a {@link java.util.function.Consumer} provided to 
 *               {@link ListenableFuture#failureCallback(java.util.function.Consumer)}
 * 
 * @since 4.4.0
 */
@Deprecated
public abstract class AbstractFutureCallbackFailureHandler implements FutureCallback<Object> {
  @Override
  public void handleResult(Object result) {
    // ignored
  }
}

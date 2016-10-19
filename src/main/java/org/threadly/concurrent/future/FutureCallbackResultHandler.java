package org.threadly.concurrent.future;

import java.util.function.Consumer;

import org.threadly.util.ArgumentVerifier;

/**
 * An implementation of {@link FutureCallback} where failures are ignored and the result is 
 * delegated to a lambda provided at construction.
 * <p>
 * See {@link AbstractFutureCallbackResultHandler} for a similar implementation except using an 
 * abstract class rather than accepting a lambda.
 * 
 * @param <T> Type of result returned
 * @since 5.0.0
 */
public class FutureCallbackResultHandler<T> extends AbstractFutureCallbackResultHandler<T> {
  private final Consumer<T> resultHandler;
  
  /**
   * Construct a new result handler with the provided consumer that results will be provided to.
   * 
   * @param resultHandler Consumer for results to accept results as callback is invoked.
   */
  public FutureCallbackResultHandler(Consumer<T> resultHandler) {
    ArgumentVerifier.assertNotNull(resultHandler, "resultHandler");
    
    this.resultHandler = resultHandler;
  }
  
  @Override
  public void handleResult(T result) {
    resultHandler.accept(result);
  }
}

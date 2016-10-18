package org.threadly.concurrent.future;

import java.util.function.Consumer;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>An implementation of {@link FutureCallback} where the result is ignored and failures are 
 * delegated to a lambda provided at construction.  This can allow you to easily do an action only 
 * on a failure condition.</p>
 * 
 * <p>See {@link AbstractFutureCallbackFailureHandler} for a similar implementation except using an 
 * abstract class rather than accepting a lambda.</p>
 * 
 * @author jent - Mike Jensen
 * @since 5.0.0
 */
public class FutureCallbackFailureHandler extends AbstractFutureCallbackFailureHandler {
  private final Consumer<Throwable> failureHandler;
  
  /**
   * Construct a new failure handler with the provided consumer that failures will be provided to.
   * 
   * @param failureHandler Handler to be invoked as callback is invoked with failures
   */
  public FutureCallbackFailureHandler(Consumer<Throwable> failureHandler) {
    ArgumentVerifier.assertNotNull(failureHandler, "failureHandler");
    
    this.failureHandler = failureHandler;
  }
  
  @Override
  public void handleFailure(Throwable t) {
    failureHandler.accept(t);
  }
}

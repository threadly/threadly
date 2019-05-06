package org.threadly.concurrent.future;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionHandler;

/**
 * Class which will map failures provided to this {@link FutureCallback} into a 
 * {@link ExceptionHandler}.  By default when constructed results will be ignored, if you wish to 
 * handle results, as well as have failures mapped you can override 
 * {@link FutureCallback#handleResult(Object)}.
 * 
 * @deprecated Instead provide the {@link ExceptionHandler} directly into 
 *               {@link ListenableFuture#failureCallback(java.util.function.Consumer)}
 * 
 * @since 4.4.0
 */
@Deprecated
public class FutureCallbackExceptionHandlerAdapter extends AbstractFutureCallbackFailureHandler {
  private final ExceptionHandler handler;
  
  /**
   * Construct a new {@link FutureCallbackExceptionHandlerAdapter} with the provided handler 
   * implementation.
   * 
   * @param handler Handler to delegate failures to
   */
  public FutureCallbackExceptionHandlerAdapter(ExceptionHandler handler) {
    ArgumentVerifier.assertNotNull(handler, "handler");
    
    this.handler = handler;
  }
  
  @Override
  public void handleFailure(Throwable t) {
    handler.handleException(t);
  }
}

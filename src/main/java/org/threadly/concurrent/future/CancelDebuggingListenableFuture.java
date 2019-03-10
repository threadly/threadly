package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.SuppressedStackRuntimeException;

/**
 * Wrapper for a {@link ListenableFuture} to provide enhanced features for debugging the state at 
 * which at which it was canceled.  When a cancel request comes in this class will attempt to 
 * record the stack trace of the delegateFuture.  If it then cancels, and is able to get a stack 
 * trace from the processing thread at time of cancellation, then any requests to {@link #get()} 
 * that result in a {@link CancellationException}, the exception will have a cause of 
 * {@link FutureProcessingStack} with the previous stack trace included.
 *
 * @since 5.28
 * @param <T> Type of result provided by this ListenableFuture
 */
public class CancelDebuggingListenableFuture<T> implements ListenableFuture<T> {
  private final ListenableFuture<T> delegateFuture;
  private StackTraceElement[] cancelStack;
  
  /**
   * Construct a new {@link CancelDebuggingListenableFuture} by wrapping the provided future.
   * 
   * @param delegateFuture A non-null future to wrap
   */
  public CancelDebuggingListenableFuture(ListenableFuture<T> delegateFuture) {
    ArgumentVerifier.assertNotNull(delegateFuture, "delegateFuture");
    
    this.delegateFuture = delegateFuture;
    cancelStack = null;
  }

  @Override
  public boolean cancel(boolean interrupt) {
    StackTraceElement[] cancelStack = 
        delegateFuture.getRunningStackTrace();  // must get stack BEFORE cancel
    if (delegateFuture.cancel(interrupt)) {
      this.cancelStack = cancelStack;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      return delegateFuture.get();
    } catch (CancellationException e) {
      prepareCancellationException(e);
      throw e;
    }
  }

  @Override
  public T get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException,
                                                TimeoutException {
    try {
      return delegateFuture.get(arg0, arg1);
    } catch (CancellationException e) {
      prepareCancellationException(e);
      throw e;
    }
  }
  
  private void prepareCancellationException(CancellationException e) {
    if (cancelStack != null) {
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      rootCause.initCause(new FutureProcessingStack(cancelStack));
    }
  }

  @Override
  public boolean isCancelled() {
    return delegateFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return delegateFuture.isDone();
  }

  @Override
  public ListenableFuture<T> listener(Runnable listener, Executor executor,
                                      ListenerOptimizationStrategy optimizeExecution) {
    delegateFuture.listener(listener, executor, optimizeExecution);
    
    return this;
  }

  @Override
  public StackTraceElement[] getRunningStackTrace() {
    return delegateFuture.getRunningStackTrace();
  }
  
  /**
   * Throwable that is not thrown, but instead added as a cause to indicate the processing stack 
   * trace at the time of cancellation.
   */
  public static class FutureProcessingStack extends SuppressedStackRuntimeException {
    private static final long serialVersionUID = 5326874345871027481L;

    protected FutureProcessingStack(StackTraceElement[] cancelStack) {
      this.setStackTrace(cancelStack);
    }
  }
}

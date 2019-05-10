package org.threadly.concurrent.future;

import static org.threadly.concurrent.future.InternalFutureUtils.invokeCompletedDirectly;

import java.util.concurrent.Executor;

/**
 * Abstract class for futures that can't be canceled and are already complete.
 * 
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
abstract class AbstractCompletedListenableFuture<T> extends AbstractNoncancelableListenableFuture<T>
                                                    implements ListenableFuture<T> {
  
  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public ListenableFuture<T> listener(Runnable listener) {
    listener.run();
    
    return this;
  }

  @Override
  public ListenableFuture<T> listener(Runnable listener, Executor executor, 
                                      ListenerOptimizationStrategy optimize) {
    if (invokeCompletedDirectly(executor, optimize)) {
      listener.run();
    } else {
      executor.execute(listener);
    }
    
    return this;
  }

  @Override
  public StackTraceElement[] getRunningStackTrace() {
    return null;
  }
}
package org.threadly.concurrent.future;

import java.util.concurrent.Executor;

/**
 * Abstract class for futures that can't be canceled and are already complete.
 * 
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
abstract class AbstractImmediateListenableFuture<T> extends AbstractNoncancelableListenableFuture<T>
                                                    implements ListenableFuture<T> {
  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public void addListener(Runnable listener) {
    listener.run();
  }

  @Override
  public void addListener(Runnable listener, Executor executor, 
                          ListenerOptimizationStrategy optimize) {
    if (executor != null && 
        optimize != ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
      executor.execute(listener);
    } else {
      listener.run();
    }
  }

  @Override
  public StackTraceElement[] getRunningStackTrace() {
    return null;
  }
}
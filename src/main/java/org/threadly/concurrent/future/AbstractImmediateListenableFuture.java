package org.threadly.concurrent.future;

import java.util.concurrent.Executor;

/**
 * <p>Abstract class for futures that can't be canceled and are already complete.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
abstract class AbstractImmediateListenableFuture<T> extends AbstractNoncancelableListenableFuture<T>
                                                    implements Promise<T> {
  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public void addListener(Runnable listener) {
    listener.run();
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    if (executor != null) {
      executor.execute(listener);
    } else {
      listener.run();
    }
  }
}
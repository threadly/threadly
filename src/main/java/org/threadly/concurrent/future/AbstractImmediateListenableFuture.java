package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.function.Function;

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
  public void addListener(Runnable listener, Executor executor) {
    if (executor != null) {
      executor.execute(listener);
    } else {
      listener.run();
    }
  }

  @Override
  public <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper) {
    return map(mapper, null);
  }

  @Override
  public <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper, Executor executor) {
    return FutureUtils.transform(this, mapper, executor);
  }

  @Override
  public <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper) {
    return flatMap(mapper, null);
  }

  @Override
  public <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper, Executor executor) {
    return FutureUtils.flatTransform(this, mapper, executor);
  }
}
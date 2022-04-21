package org.threadly.concurrent.future;

import static org.threadly.concurrent.future.InternalFutureUtils.invokeCompletedDirectly;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.threadly.util.StackSuppressedRuntimeException;

@SuppressWarnings("javadoc")
public class ListenableFutureDefaultTest extends ListenableFutureInterfaceTest {
  @Override
  protected ListenableFutureFactory makeListenableFutureFactory() {
    return new MinimalListenableFutureFactory();
  }
  
  private static class MinimalListenableFutureFactory implements ListenableFutureFactory {
    @Override
    public ListenableFuture<?> makeCanceled() {
      return new MinimalListenableFuture<>();
    }

    @Override
    public ListenableFuture<Object> makeWithFailure(Exception e) {
      return new MinimalListenableFuture<>(e);
    }

    @Override
    public <T> ListenableFuture<T> makeWithResult(T result) {
      return new MinimalListenableFuture<>(result);
    }
  }
  
  /**
   * Class which represents the minimal functions needed to be implemented for 
   * {@link ListenableFuture}.  Since Threadly often overrides these default functions for more 
   * optimized implementations, this allows us to make sure we can directly test the defaults.
   */
  private static class MinimalListenableFuture<T> implements ListenableFuture<T> {
    private final boolean canceled;
    private final T result;
    private final Throwable t;
    
    public MinimalListenableFuture() {
      this.canceled = true;
      this.result = null;
      this.t = null;
    }
    
    public MinimalListenableFuture(T result) {
      this.canceled = false;
      this.result = result;
      this.t = null;
    }
    
    public MinimalListenableFuture(Throwable t) {
      this.canceled = false;
      this.result = null;
      this.t = t == null ? new StackSuppressedRuntimeException() : t;
    }
    
    @Override
    public boolean cancel(boolean interrupt) {
      return false;
    }

    @Override
    public T get() throws ExecutionException {
      if (canceled) {
        throw new CancellationException();
      } else if (t != null) {
        throw new ExecutionException(t);
      } else {
        return result;
      }
    }

    @Override
    public T get(long arg0, TimeUnit arg1) throws ExecutionException {
      return get();
    }

    @Override
    public boolean isCancelled() {
      return canceled;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public boolean isCompletedExceptionally() {
      return t != null || canceled;
    }

    @Override
    public Throwable getFailure() {
      if (canceled) {
        return new CancellationException();
      } else {
        return t;
      }
    }

    @Override
    public Throwable getFailure(long timeout, TimeUnit unit) {
      return getFailure();
    }

    @Override
    public ListenableFuture<T> listener(Runnable listener, Executor executor,
                                        ListenerOptimizationStrategy optimizeExecution) {
      if (invokeCompletedDirectly(executor, optimizeExecution)) {
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
}

package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("javadoc")
public class TestFutureImp implements ListenableFuture<Object> {
  public List<Runnable> listeners = new ArrayList<>(1);
  public final Object result = new Object();
  protected boolean canceled = false;
  private final boolean runListeners;
  
  public TestFutureImp(boolean runListeners) {
    this.runListeners = runListeners;
  }
  
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    canceled = true;
    
    return true;
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
    return canceled;
  }

  @Override
  public Object get() throws ExecutionException {
    if (canceled) {
      throw new CancellationException();
    }
    return result;
  }

  @Override
  public Object get(long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
    if (canceled) {
      throw new CancellationException();
    }
    return result;
  }

  @Override
  public Throwable getFailure() {
    if (canceled) {
      return new CancellationException();
    }
    return null;
  }

  @Override
  public Throwable getFailure(long timeout, TimeUnit unit) throws TimeoutException {
    if (canceled) {
      return new CancellationException();
    }
    return null;
  }

  @Override
  public ListenableFuture<Object> listener(Runnable listener) {
    if (runListeners) {
      listener.run();
    } else {
      listeners.add(listener);
    }
    
    return this;
  }

  @Override
  public ListenableFuture<Object> listener(Runnable listener, Executor executor, 
                                           ListenerOptimizationStrategy optimizeExecution) {
    return listener(listener);
  }

  @Override
  public StackTraceElement[] getRunningStackTrace() {
    return null;
  }
}
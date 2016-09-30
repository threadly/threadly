package org.threadly.concurrent.future;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@SuppressWarnings("javadoc")
public class TestFutureImp implements ListenableFuture<Object> {
  public final Object result = new Object();
  protected boolean canceled = false;
  protected List<Runnable> listeners = new ArrayList<>(1);
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
  public Object get() throws ExecutionException {
    if (canceled) {
      throw new CancellationException();
    }
    return result;
  }

  @Override
  public Object get(long timeout, TimeUnit unit) throws TimeoutException {
    if (canceled) {
      throw new CancellationException();
    }
    return result;
  }

  @Override
  public void addListener(Runnable listener) {
    if (runListeners) {
      listener.run();
    } else {
      listeners.add(listener);
    }
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    addListener(listener);
  }

  @Override
  public void addCallback(FutureCallback<? super Object> callback) {
    addCallback(callback, null);
  }

  @Override
  public void addCallback(FutureCallback<? super Object> callback, Executor executor) {
    addListener(new RunnableFutureCallbackAdapter<>(this, callback), executor);
  }

  @Override
  public <R> ListenableFuture<R> map(Function<Object, R> mapper) {
    return map(mapper, null);
  }

  @Override
  public <R> ListenableFuture<R> map(Function<Object, R> mapper, Executor executor) {
    return FutureUtils.transform(this, mapper, executor);
  }
}
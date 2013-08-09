package org.threadly.concurrent.future;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("javadoc")
public class TestFutureImp implements ListenableFuture<Object> {
  public final Object result = new Object();
  protected boolean canceled = false;
  protected List<Runnable> listeners = new LinkedList<Runnable>();
  
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    canceled = true;
    
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public Object get() throws InterruptedException, ExecutionException {
    return result;
  }

  @Override
  public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return result;
  }

  @Override
  public void addListener(Runnable listener) {
    listeners.add(listener);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    listeners.add(listener);
  }
}
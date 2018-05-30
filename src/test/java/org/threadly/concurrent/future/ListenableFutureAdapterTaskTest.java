package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.ExceptionUtils;

@SuppressWarnings("javadoc")
public class ListenableFutureAdapterTaskTest extends ThreadlyTester {
  @Test
  public void resultTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFutureAdapterTask<Object> adapter = 
        new ListenableFutureAdapterTask<>(FutureUtils.immediateResultFuture(result));
    
    adapter.run();

    assertTrue(adapter.isDone());
    assertTrue(result == adapter.get());
  }
  
  @Test
  public void cancelTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    slf.cancel(false);
    ListenableFutureAdapterTask<Object> adapter = new ListenableFutureAdapterTask<>(slf);
    
    adapter.run();
    
    assertTrue(adapter.isCancelled());
  }
  
  @Test
  public void exceptionFailureTest() throws InterruptedException {
    Exception failure = new Exception();
    ListenableFutureAdapterTask<Object> adapter = 
        new ListenableFutureAdapterTask<>(FutureUtils.immediateFailureFuture(failure));
    
    adapter.run();

    assertTrue(adapter.isDone());
    try {
      adapter.get();
      fail("Exception shuld have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void throwableFailureTest() throws InterruptedException {
    Throwable failure = new Throwable();
    ListenableFutureAdapterTask<Object> adapter = 
        new ListenableFutureAdapterTask<>(FutureUtils.immediateFailureFuture(failure));
    
    adapter.run();

    assertTrue(adapter.isDone());
    try {
      adapter.get();
      fail("Exception shuld have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == ExceptionUtils.getRootCause(e));
    }
  }
}

package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class CompletableFutureAdapterTest {
  @Test
  public void adaptCompletedResultListenableFutureTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFuture<?> lf = FutureUtils.immediateResultFuture(result);
    
    CompletableFuture<Object> cf = CompletableFutureAdapter.toCompletable(lf);
    
    assertTrue(cf.isDone());
    assertTrue(result == cf.get());
  }
  
  @Test
  public void adaptCompletedFailureListenableFutureTest() throws InterruptedException {
    Exception failure = new Exception();
    ListenableFuture<?> lf = FutureUtils.immediateFailureFuture(failure);
    
    CompletableFuture<Object> cf = CompletableFutureAdapter.toCompletable(lf);
    
    assertTrue(cf.isDone());
    assertTrue(cf.isCompletedExceptionally());
    try {
      cf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void adaptIncompletedResultListenableFutureTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    SettableListenableFuture<Object> lf = new SettableListenableFuture<>();
    
    CompletableFuture<Object> cf = CompletableFutureAdapter.toCompletable(lf);

    assertFalse(cf.isDone());
    lf.setResult(result);
    assertTrue(cf.isDone());
    assertTrue(result == cf.get());
  }
  
  @Test
  public void adaptInompletedFailureListenableFutureTest() throws InterruptedException {
    Exception failure = new Exception();
    SettableListenableFuture<Object> lf = new SettableListenableFuture<>();
    
    CompletableFuture<Object> cf = CompletableFutureAdapter.toCompletable(lf);

    assertFalse(cf.isDone());
    lf.setFailure(failure);
    assertTrue(cf.isDone());
    assertTrue(cf.isCompletedExceptionally());
    try {
      cf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void adaptCompletedResultCompletableFutureTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    CompletableFuture<Object> cf = CompletableFuture.completedFuture(result);
    
    ListenableFuture<?> lf = CompletableFutureAdapter.toListenable(cf);
    
    assertTrue(lf.isDone());
    assertTrue(result == lf.get());
  }
  
  @Test
  public void adaptCompletedFailureCompletableFutureTest() throws InterruptedException {
    Exception failure = new Exception();
    CompletableFuture<Object> cf = new CompletableFuture<>();
    cf.completeExceptionally(failure);

    ListenableFuture<?> lf = CompletableFutureAdapter.toListenable(cf);
    
    assertTrue(lf.isDone());
    try {
      lf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void adaptIncompletedResultCompletableFutureTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    CompletableFuture<Object> cf = new CompletableFuture<>();

    ListenableFuture<?> lf = CompletableFutureAdapter.toListenable(cf);

    assertFalse(lf.isDone());
    cf.complete(result);
    assertTrue(lf.isDone());
    assertTrue(result == lf.get());
  }
  
  @Test
  public void adaptInompletedFailureCompletableFutureTest() throws InterruptedException {
    Exception failure = new Exception();
    CompletableFuture<Object> cf = new CompletableFuture<>();

    ListenableFuture<?> lf = CompletableFutureAdapter.toListenable(cf);

    assertFalse(lf.isDone());
    cf.completeExceptionally(failure);
    assertTrue(lf.isDone());
    try {
      lf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void adaptBackToListenableFutureTest() {
    ListenableFuture<?> lf = FutureUtils.immediateResultFuture(new Object());
    CompletableFuture<Object> cf = CompletableFutureAdapter.toCompletable(lf);
    
    assertTrue(cf == CompletableFutureAdapter.toListenable(cf));
  }
  
  @Test
  public void adaptedListenableFutureListenerTest() {
    // adapt twice so we are using the CompletableFuture implementation (as tested in adaptBackToListenableFutureTest)
    ListenableFuture<?> lf = 
        CompletableFutureAdapter.toListenable(
            CompletableFutureAdapter.toCompletable(
                FutureUtils.immediateResultFuture(new Object())));
    
    TestRunnable tr = new TestRunnable();
    assertTrue(lf == lf.listener(tr));
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void adaptedListenableFutureGetRunningStackTest() {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    slf.setRunningThread(Thread.currentThread());
    // adapt twice so we are using the CompletableFuture implementation (as tested in adaptBackToListenableFutureTest)
    ListenableFuture<?> lf = 
        CompletableFutureAdapter.toListenable(
            CompletableFutureAdapter.toCompletable(slf));
    
    assertNotNull(lf.getRunningStackTrace());
  }
}

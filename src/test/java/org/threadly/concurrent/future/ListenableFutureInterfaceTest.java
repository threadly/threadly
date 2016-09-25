package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.StringUtils;
import org.threadly.util.SuppressedStackRuntimeException;

@SuppressWarnings("javadoc")
public abstract class ListenableFutureInterfaceTest {
  protected abstract ListenableFutureFactory makeListenableFutureFactory();
  
  @Test
  public void addCallbackAlreadyDoneFutureTest() {
    String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(result);
    
    TestFutureCallback tfc = new TestFutureCallback();
    lf.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackExecutionExceptionAlreadyDoneTest() {
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    
    TestFutureCallback tfc = new TestFutureCallback();
    lf.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void addListenerAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    
    TestRunnable tr = new TestRunnable();
    lf.addListener(tr);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void mapAlreadyDoneTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.map((s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    });
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void mapAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    
    try {
      mappedLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void mapAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void mapAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.map((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    
    try {
      mappedLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void mapWithExecutorAlreadyDoneTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.map((s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    }, scheduler);
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void mapWithExecutorAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    
    try {
      mappedLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void mapWithExecutorAlreadyCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(0, scheduler.tick());

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void mapWithExecutorAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.map((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    
    try {
      mappedLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  protected interface ListenableFutureFactory {
    public ListenableFuture<?> makeCanceled();
    public ListenableFuture<?> makeWithFailure(Exception e);
    public <T> ListenableFuture<T> makeWithResult(T result);
  }
}

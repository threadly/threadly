package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.test.concurrent.TestCondition;
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
    verifyFutureFailure(mappedLF, failure);
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
    verifyFutureFailure(mappedLF, failure);
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
    verifyFutureFailure(mappedLF, failure);
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
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void cancelWhileMappedFunctionRunningInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean interrupted = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.map((o) -> {
        started.set(true);
        try {
          Thread.sleep(10_000);
        } catch (InterruptedException e) {
          interrupted.set(true);
        }
        return null;
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertTrue(mappedLF.cancel(true));
      new TestCondition(() -> interrupted.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void cancelWhileMappedFunctionRunningNoInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean completed = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.map((o) -> {
        started.set(true);
        try {
          Thread.sleep(DELAY_TIME * 10);
          completed.set(true);
        } catch (InterruptedException e) {
          // should not occur, if it does completed will never set
        }
        return null;
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertFalse(mappedLF.cancel(false));
      new TestCondition(() -> completed.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void flatMapAlreadyDoneTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.flatMap((s) -> {
      if (s == sourceObject) {
        return FutureUtils.immediateResultFuture(translatedObject);
      } else {
        // test failure
        return FutureUtils.immediateResultFuture(null);
      }
    });
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void flatMapAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    });

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void flatMapAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapAlreadyDoneMapperReturnFailedFutureTest() throws InterruptedException {
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> FutureUtils.immediateFailureFuture(failure));

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void cancelWhileFlatMappedMapFunctionRunningInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean interrupted = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
        started.set(true);
        try {
          Thread.sleep(10_000);
        } catch (InterruptedException e) {
          interrupted.set(true);
        }
        return FutureUtils.immediateResultFuture(null);
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertTrue(mappedLF.cancel(true));
      new TestCondition(() -> interrupted.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void cancelWhileFlatMappedMapFunctionRunningNoInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean completed = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
        started.set(true);
        try {
          Thread.sleep(DELAY_TIME * 10);
          completed.set(true);
        } catch (InterruptedException e) {
          // should not occur, if it does completed will never set
        }
        return FutureUtils.immediateResultFuture(null);
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertFalse(mappedLF.cancel(false));
      new TestCondition(() -> completed.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void cancelWhileFlatMappedMapFutureIncompleteRunningInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> scheduledFuture = sts.submitScheduled(DoNothingRunnable.instance(), null, 10_000);
      ListenableFuture<Void> mappedLF = lf.flatMap((o) -> scheduledFuture);
      
      assertTrue(mappedLF.cancel(false)); // no interrupt needed, delegate future not started
      assertTrue(scheduledFuture.isCancelled());
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.flatMap((s) -> {
      if (s == sourceObject) {
        return FutureUtils.immediateResultFuture(translatedObject);
      } else {
        // test failure
        return FutureUtils.immediateResultFuture(null);
      }
    }, scheduler);
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    }, scheduler);
    
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapWithExecutorAlreadyCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    }, scheduler);
    
    assertEquals(0, scheduler.tick());

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneMapperReturnFailedFutureTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> FutureUtils.immediateFailureFuture(failure), scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  private static void verifyFutureFailure(ListenableFuture<?> f, Exception failure) throws InterruptedException {
    try {
      f.get();
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

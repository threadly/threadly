package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.future.ListenableFuture.ListenerOptimizationStrategy;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;
import org.threadly.util.SuppressedStackRuntimeException;
import org.threadly.util.TestExceptionHandler;

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
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setDefaultExceptionHandler(teh);
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.map((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(1, teh.getCallCount());
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
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setDefaultExceptionHandler(teh);
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.map((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(1, teh.getCallCount());
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
  public void throwMapAlreadyDoneTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.throwMap((s) -> {
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
  public void throwMapAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void throwMapAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void throwMapAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setDefaultExceptionHandler(teh);
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(0, teh.getCallCount());
  }
  
  @Test
  public void throwMapWithExecutorAlreadyDoneTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.throwMap((s) -> {
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
  public void throwMapWithExecutorAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    Exception failure = new Exception();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void throwMapWithExecutorAlreadyCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(0, scheduler.tick());

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void throwMapWithExecutorAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setDefaultExceptionHandler(teh);
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(0, teh.getCallCount());
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
  
  @Test
  public void mapFailureIgnoredTest() {
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = lf.mapFailure(Throwable.class, 
                                                     (t) -> { mapped.set(true); return new Object(); });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void mapFailureIgnoredFailureTypeTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(RuntimeException.class, 
                                                (t) -> { mapped.set(true); return null; });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void mapFailureIntoExceptionTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); throw finalException; });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  private static void verifyFailureConditionFuture(ListenableFuture<?> finalLF, 
                                                   Exception expectedFailure) throws InterruptedException {
    assertTrue(finalLF.isDone());
    try {
      finalLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() == expectedFailure);
    }
  }
  
  @Test
  public void mapFailureIntoResultTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); return null; });

    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    assertNull(finalLF.get());
  }
  
  @Test
  public void flatMapFailureIgnoredTest() {
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Throwable.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(new Object()); });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void flatMapFailureIgnoredFailureTypeTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(RuntimeException.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void flatMapFailureIntoThrownExceptionTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.flatMapFailure(Exception.class, 
                                                    (t) -> { mapped.set(true); throw finalException; });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureIntoReturnedExceptionTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateFailureFuture(finalException); });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureIntoResultTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); });

    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    assertNull(finalLF.get());
  }
  
  @Test
  public void mapFailureWithExecutorIgnoredTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = lf.mapFailure(Throwable.class, 
                                                     (t) -> { mapped.set(true); return new Object(); }, 
                                                     scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertFalse(mapped.get());
    assertTrue(finalLF.isDone());
  }
  
  @Test
  public void mapFailureWithExecutorIgnoredFailureTypeTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(RuntimeException.class, 
                                                (t) -> { mapped.set(true); return null; }, 
                                                scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertFalse(mapped.get());
    assertTrue(finalLF.isDone());
  }
  
  @Test
  public void mapFailureWithExecutorIntoExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); throw finalException; }, 
                                                scheduler);

    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void mapFailureWithExecutorIntoResultTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); return null; }, 
                                                scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertNull(finalLF.get());
  }
  
  @Test
  public void flatMapFailureWithExecutorIgnoredTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Throwable.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(new Object()); }, 
                          scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertFalse(mapped.get());
    assertTrue(finalLF.isDone());
  }
  
  @Test
  public void flatMapFailureWithExecutorIgnoredFailureTypeTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(RuntimeException.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); }, 
                          scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertFalse(mapped.get());
    assertTrue(finalLF.isDone());
  }
  
  @Test
  public void flatMapFailureWithExecutorIntoThrownExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.flatMapFailure(Exception.class, 
                                                    (t) -> { mapped.set(true); throw finalException; }, 
                                                    scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureWithExecutorIntoReturnedExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateFailureFuture(finalException); }, 
                          scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureWithExecutorIntoResultTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); }, 
                          scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertNull(finalLF.get());
  }

  @Test
  public void optimizeDoneListenerExecutorTest() throws InterruptedException, TimeoutException {
    optimizeDoneListenerExecutorTest(makeListenableFutureFactory().makeWithResult(null));
  }

  @Test
  public void dontOptimizeDoneListenerExecutorTest() throws InterruptedException, TimeoutException {
    dontOptimizeDoneListenerExecutorTest(makeListenableFutureFactory().makeWithResult(null));
  }
  
  public static void optimizeDoneListenerExecutorTest(ListenableFuture<?> lf) throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    Thread t = Thread.currentThread();
    
    lf.addListener(() -> {av.assertTrue(Thread.currentThread() == t) ; av.signalComplete();}, 
                   CentralThreadlyPool.computationPool(), 
                   ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);
    
    av.waitForTest();
  }
  
  public static void dontOptimizeDoneListenerExecutorTest(ListenableFuture<?> lf) throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    Thread t = Thread.currentThread();
    Runnable threadTester = 
        () -> {av.assertFalse(Thread.currentThread() == t) ; av.signalComplete();};

    lf.addListener(threadTester, CentralThreadlyPool.computationPool(), null);
    lf.addListener(threadTester, CentralThreadlyPool.computationPool(), 
                   ListenerOptimizationStrategy.None);
    lf.addListener(threadTester, CentralThreadlyPool.computationPool(), 
                   ListenerOptimizationStrategy.SingleThreadIfExecutorMatch);
    
    av.waitForTest(10_000, 3);
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
    public ListenableFuture<Object> makeWithFailure(Exception e);
    public <T> ListenableFuture<T> makeWithResult(T result);
  }
}

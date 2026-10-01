package org.threadly.concurrent.future;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StackSuppressedRuntimeException;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class SettableListenableFutureTest extends CompletableListenableFutureInterfaceTest {
  protected SettableListenableFuture<String> slf;
  
  @BeforeEach
  public void setup() {
    slf = new SettableListenableFuture<>();
  }
  
  @AfterEach
  @Override
  public void cleanup() {
    slf = null;
  }

  @Override
  protected CompletableListenableFutureFactory makeCompletableListenableFutureFactory() {
    return new SettableListenableFutureFactory();
  }
  
  @Test
  public void setResultResultFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.setResult(null);
      slf.setResult(null);
      });
  }
  
  @Test
  public void setResultResultTest() {
    slf = new SettableListenableFuture<>(false);
    assertTrue(slf.setResult(null));
    assertFalse(slf.setResult(null));
  }
  
  @Test
  public void setFailureResultFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.setFailure(new StackSuppressedRuntimeException());
      slf.setResult(null);
      });
  }
  
  @Test
  public void setFailureResultTest() {
    slf = new SettableListenableFuture<>(false);
    assertTrue(slf.setFailure(new StackSuppressedRuntimeException()));
    assertFalse(slf.setResult(null));
  }
  
  @Test
  public void setResultFailureFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.setResult(null);
      slf.setFailure(null);
      });
  }
  
  @Test
  public void setResultFailureTest() {
    slf = new SettableListenableFuture<>(false);
    assertTrue(slf.setResult(null));
    assertFalse(slf.setFailure(null));
  }
  
  @Test
  public void setFailureFailureFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.setFailure(new StackSuppressedRuntimeException());
      slf.setFailure(null);
      });
  }
  
  @Test
  public void setFailureFailureTest() {
    slf = new SettableListenableFuture<>(false);
    assertTrue(slf.setFailure(new StackSuppressedRuntimeException()));
    assertFalse(slf.setFailure(null));
  }
  
  @Test
  public void cancelSetResultFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.cancel(false);
      slf.setResult(null);
      });
  }
  
  @Test
  public void cancelSetResultTest() {
    slf = new SettableListenableFuture<>(false);
    assertTrue(slf.cancel(false));
    assertFalse(slf.setResult(null));
  }
  
  @Test
  public void cancelSetFailureFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.cancel(false);
      slf.setFailure(null);
      });
  }
  
  @Test
  public void cancelSetFailureTest() {
    slf = new SettableListenableFuture<>(false);
    assertTrue(slf.cancel(false));
    assertFalse(slf.setFailure(null));
  }
  
  @Test
  public void isCompletedExceptionallyWithFailureTest() {
    assertFalse(slf.isCompletedExceptionally());
    slf.setFailure(new StackSuppressedRuntimeException());
    assertTrue(slf.isCompletedExceptionally());
  }
  
  @Test
  public void isCompletedExceptionallyWithResultTest() {
    slf.setResult(null);
    assertFalse(slf.isCompletedExceptionally());
  }
  
  @Test
  public void isDoneByCancelTest() {
    assertFalse(slf.isDone());
    
    assertTrue(slf.cancel(false));
    
    assertTrue(slf.isDone());
    assertTrue(slf.isCompletedExceptionally());
  }
  
  @Override
  @Test
  public void clearResultFail() {
      assertThrows(IllegalStateException.class, () -> {
      slf.clearResult();
      });
  }
  
  @Test
  public void getAfterClearResultFail(){
      assertThrows(IllegalStateException.class, () -> {
      slf.setResult(null);
      slf.clearResult();
      slf.get();
      });
  }
  
  @Test
  public void getAfterClearFailureResultFail(){
      assertThrows(IllegalStateException.class, () -> {
      slf.setFailure(new StackSuppressedRuntimeException());
      slf.clearResult();
      slf.get();
      });
  }
  
  @Test
  public void listenersCalledOnResultTest() {
    listenersCalledTest(false);
  }
  
  @Test
  public void listenersCalledOnFailureTest() {
    listenersCalledTest(true);
  }
  
  private void listenersCalledTest(boolean failure) {
    TestRunnable tr = new TestRunnable();
    slf.listener(tr);
    
    if (failure) {
      slf.setFailure(new StackSuppressedRuntimeException());
    } else {
      slf.setResult(null);
    }
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    slf.listener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void callbackTest() {
    String result = StringUtils.makeRandomString(5);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.callback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setResult(result);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void resultCallbackTest() {
    String result = StringUtils.makeRandomString(5);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.resultCallback(tfc::handleResult);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setResult(result);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void callbackExecutionExceptionTest() {
    Throwable failure = new StackSuppressedRuntimeException();
    TestFutureCallback tfc = new TestFutureCallback();
    slf.callback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setFailure(failure);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void failureCallbackExecutionExceptionTest() {
    Throwable failure = new StackSuppressedRuntimeException();
    TestFutureCallback tfc = new TestFutureCallback();
    slf.failureCallback(tfc::handleFailure);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setFailure(failure);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void addAsCallbackResultTest() throws InterruptedException, ExecutionException {
    String testResult = StringUtils.makeRandomString(5);
    ListenableFuture<String> resultFuture = new ImmediateResultListenableFuture<>(testResult);
    
    resultFuture.callback(slf);
    
    assertTrue(slf.isDone());
    assertEquals(testResult, slf.get());
  }
  
  @Test
  public void addAsCallbackFailureTest() throws InterruptedException {
    Exception e = new StackSuppressedRuntimeException();
    ListenableFuture<String> failureFuture = new ImmediateFailureListenableFuture<>(e);
    
    failureFuture.callback(slf);
    
    assertTrue(slf.isDone());
    try {
      slf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() == e);
    }
  }
  
  @Test
  public void cancelTest() {
    assertTrue(slf.cancel(false));
    assertFalse(slf.cancel(false));
    
    assertTrue(slf.isCancelled());
    assertTrue(slf.isDone());
  }
  
  @Test
  public void cancelRunsListenersTest() {
    TestRunnable tr = new TestRunnable();
    slf.listener(tr);
    
    slf.cancel(false);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void cancelWithInterruptTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    Thread runningThread = new Thread(btr);
    try {
      runningThread.start();
      slf.setRunningThread(runningThread);
      btr.blockTillStarted();
      
      assertTrue(slf.cancel(true));
      
      // should unblock when interrupted
      btr.blockTillFinished();
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void isDoneTest() {
    assertFalse(slf.isDone());
    
    slf.setResult(null);

    assertTrue(slf.isDone());
  }
  
  @Test
  public void getAsyncResultTest() throws InterruptedException, ExecutionException {
    final String testResult = StringUtils.makeRandomString(5);
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1);
    try {
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          slf.setResult(testResult);
        }
      }, DELAY_TIME);
      
      assertTrue(slf.get() == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAsyncResultWithTimeoutTest() throws InterruptedException, 
                                                     ExecutionException, 
                                                     TimeoutException {
    final String testResult = StringUtils.makeRandomString(5);
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1);
    try {
      scheduler.prestartAllThreads();
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          slf.setResult(testResult);
        }
      }, DELAY_TIME);
      
      assertTrue(slf.get(DELAY_TIME + (SLOW_MACHINE ? 2000 : 1000), TimeUnit.MILLISECONDS) == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void chainCanceledFutureTest() {
    SettableListenableFuture<String> canceledSlf = new SettableListenableFuture<>();
    assertTrue(canceledSlf.cancel(false));
    
    canceledSlf.callback(slf);
    
    assertTrue(slf.isCancelled());
  }
  
  @Test
  public void cancelChainedFutureTest() {
    SettableListenableFuture<String> canceledSlf = new SettableListenableFuture<>();
    
    canceledSlf.callback(slf);
    assertTrue(canceledSlf.cancel(false));
    
    assertTrue(slf.isCancelled());
  }
  
  private static void verifyCancelationExceptionMessageOnGet(String msg, ListenableFuture<?> lf) throws InterruptedException {
    try {
      lf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertEquals(msg, e.getCause().getMessage());
    } catch (CancellationException e) {
      assertEquals(msg, e.getMessage());
    }
  }
  
  private static void verifyCancelationExceptionMessageInCallback(String msg, ListenableFuture<?> lf) throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    lf.callback(new FutureCallback<Object>() {
      @Override
      public void handleResult(Object result) {
        av.fail();
      }

      @Override
      public void handleFailure(Throwable t) {
        av.assertEquals(msg, t.getMessage());
        av.signalComplete();
      }
    });
    av.waitForTest();
  }
  
  @Test
  public void cancelationExceptionMessageTest() throws InterruptedException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    slf.cancel(false);

    assertTrue(slf.isDone());
    verifyCancelationExceptionMessageOnGet(msg, slf);
  }
  
  @Test
  public void futureCallbackCancelationExceptionMessageTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    slf.cancel(false);

    assertTrue(slf.isDone());
    verifyCancelationExceptionMessageInCallback(msg, slf);
  }
  
  @Test
  public void mapCancelationExceptionMessageAlreadyDoneTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    slf.cancel(false);
    ListenableFuture<Void> mappedFuture = slf.map((v) -> v);

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void mapCancelationExceptionMessageTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    ListenableFuture<Void> mappedFuture = slf.map((v) -> v);
    slf.cancel(false);

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void mapStackSizeTest() throws InterruptedException, TimeoutException {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    ListenableFutureInterfaceTest.mapStackDepthTest(slf, () -> slf.setResult(null), 53, 37);
  }
  
  @Test
  public void mapFailureStackSize() throws InterruptedException, TimeoutException {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    ListenableFutureInterfaceTest.mapFailureStackDepthTest(slf, () -> slf.setFailure(new RuntimeException()), 53);
  }
  
  @Test
  public void flatMapCancelationExceptionMessageAlreadyDoneTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    slf.cancel(false);
    ListenableFuture<Void> mappedFuture = slf.flatMap((v) -> FutureUtils.immediateResultFuture(null));

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void flatMapCancelationExceptionMessageTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    ListenableFuture<Void> mappedFuture = slf.flatMap((v) -> FutureUtils.immediateResultFuture(null));
    slf.cancel(false);

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void flatMapStackSizeTest() throws InterruptedException, TimeoutException {
    SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
    ListenableFutureInterfaceTest.flatMapStackDepthTest(slf, () -> slf.setResult(null), 73, 15);
  }
  
  @Test
  public void failureMapCancelationExceptionMessageAlreadyDoneTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    slf.cancel(false);
    ListenableFuture<Void> mappedFuture = slf.mapFailure(CancellationException.class, 
                                                         (c) -> { 
                                                           throw c;
                                                         });

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void failureMapCancelationExceptionMessageTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    ListenableFuture<Void> mappedFuture = slf.mapFailure(CancellationException.class, 
                                                         (c) -> {
                                                           throw c;
                                                         });
    slf.cancel(false);

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void failureFlatMapCancelationExceptionMessageAlreadyDoneTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    slf.cancel(false);
    ListenableFuture<Void> mappedFuture = slf.flatMapFailure(CancellationException.class, 
                                                             (c) -> FutureUtils.immediateFailureFuture(c));

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void failureFlatMapCancelationExceptionMessageTest() throws InterruptedException, TimeoutException {
    String msg = StringUtils.makeRandomString(5);
    SettableListenableFuture<Void> slf = new CancelMessageTestSettableListenableFuture(msg);
    ListenableFuture<Void> mappedFuture = slf.flatMapFailure(CancellationException.class, 
                                                             (c) -> FutureUtils.immediateFailureFuture(c));
    slf.cancel(false);

    assertTrue(mappedFuture.isDone());
    verifyCancelationExceptionMessageOnGet(msg, mappedFuture);
    verifyCancelationExceptionMessageInCallback(msg, mappedFuture);
  }
  
  @Test
  public void getRunningStackTraceTest() {
    assertNull(slf.getRunningStackTrace());
    
    slf.setRunningThread(Thread.currentThread());
    StackTraceElement[] stack = slf.getRunningStackTrace();
    assertEquals(this.getClass().getName(), stack[3].getClassName());

    slf.setResult(null);
    assertNull(slf.getRunningStackTrace());
  }
  
  @Test
  public void getMappedRunningStackTraceTest() {
    ListenableFuture<Object> mappedFuture = slf.map((o) -> o).map((o) -> null);
    
    assertNull(mappedFuture.getRunningStackTrace());
    
    slf.setRunningThread(Thread.currentThread());
    StackTraceElement[] stack = mappedFuture.getRunningStackTrace();
    assertEquals(this.getClass().getName(), stack[5].getClassName());

    slf.setResult(null);
    assertNull(mappedFuture.getRunningStackTrace());
  }
  
  @Test
  public void getFlatMappedRunningStackTraceTest() {
    ListenableFuture<Object> mappedFuture = 
        slf.flatMap((o) -> FutureUtils.immediateResultFuture(o))
           .flatMap((o) -> FutureUtils.immediateResultFuture(null));
    
    assertNull(mappedFuture.getRunningStackTrace());
    
    slf.setRunningThread(Thread.currentThread());
    StackTraceElement[] stack = mappedFuture.getRunningStackTrace();
    assertEquals(this.getClass().getName(), stack[5].getClassName());

    slf.setResult(null);
    assertNull(mappedFuture.getRunningStackTrace());
  }
  
  private static class SettableListenableFutureFactory implements CompletableListenableFutureFactory {
    @Override
    public <T> SettableListenableFuture<T> makeNewCompletable() {
      return new SettableListenableFuture<>();
    }

    @Override
    public <T> SettableListenableFuture<T> makeNewCompletable(Executor executor) {
      return new SettableListenableFuture<>(true, executor);
    }
    
    @Override
    public SettableListenableFuture<?> makeCanceledCompletable() {
      SettableListenableFuture<?> slf = new SettableListenableFuture<>();
      slf.cancel(false);
      return slf;
    }
    
    @Override
    public SettableListenableFuture<Object> makeWithFailureCompletable(Exception e) {
      SettableListenableFuture<Object> slf = new SettableListenableFuture<>();
      slf.handleFailure(e);
      return slf;
    }

    @Override
    public <T> SettableListenableFuture<T> makeWithResultCompletable(T result) {
      SettableListenableFuture<T> slf = new SettableListenableFuture<>();
      slf.handleResult(result);
      return slf;
    }
  }
  
  private static class CancelMessageTestSettableListenableFuture extends SettableListenableFuture<Void> {
    private final String msg;
    
    public CancelMessageTestSettableListenableFuture(String msg) {
      this.msg = msg;
    }
    
    @Override
    protected String getCancellationExceptionMessage() {
      return msg;
    }
  }
}

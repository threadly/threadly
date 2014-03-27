package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.concurrent.future.RunnableFutureTest.FutureFactory;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ListenableFutureTaskTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @Test
  public void getCallableResultTest() throws InterruptedException, ExecutionException {
    RunnableFutureTest.getCallableResultTest(new Factory());
  }
  
  @Test
  public void getRunnableResultTest() throws InterruptedException, ExecutionException {
    RunnableFutureTest.getRunnableResultTest(new Factory());
  }
  
  @Test
  public void getTimeoutFail() throws InterruptedException, ExecutionException {
    RunnableFutureTest.getTimeoutFail(new Factory());
  }
  
  @Test
  public void isDoneTest() {
    RunnableFutureTest.isDoneTest(new Factory());
  }
  
  @Test
  public void isDoneFail() {
    RunnableFutureTest.isDoneFail(new Factory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addListenerFail() {
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, new TestRunnable(), null);
    
    future.addListener(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void listenerTest() {
    TestRunnable tr = new TestRunnable();
    
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, tr, null);
    
    assertEquals(0, future.listenerHelper.registeredListenerCount()); // empty to start
    
    TestRunnable listener = new TestRunnable();
    
    future.addListener(listener);
    
    assertEquals(1, future.listenerHelper.registeredListenerCount()); // should now have once now that the runnable has not run yet
    
    future.run(); // this should call the listener
    
    assertTrue(listener.ranOnce()); // verify listener was called
    
    assertEquals(0, future.listenerHelper.registeredListenerCount()); // empty after listener calls
    
    TestRunnable postRunListener = new TestRunnable();
    
    future.addListener(postRunListener);
    
    assertTrue(postRunListener.ranOnce()); // verify listener was called
    
    assertEquals(0, future.listenerHelper.registeredListenerCount()); // still empty after future ran
    
    // verify run on correct executor
    TestRunnable executorListener = new TestRunnable();
    TestExecutor executor = new TestExecutor();
    future.addListener(executorListener, executor);
    
    assertEquals(1, executor.providedRunnables.size());
    assertTrue(executor.providedRunnables.get(0) == executorListener);
  }
  
  @Test
  public void listenerExceptionAddBeforeRunTest() {
    TestRunnable listener = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, new TestRunnable(), null);
    
    future.addListener(listener);
    future.run();
    
    assertTrue(listener.ranOnce());
  }
  
  @Test
  public void listenerExceptionAddAfterRunTest() {
    TestRunnable listener = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, new TestRunnable(), null);
    
    future.run();
    try {
      future.addListener(listener);
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      // expected
    }
    
    assertTrue(listener.ranOnce());
  }
  
  @Test
  public void addCallbackTest() {
    TestCallable tc = new TestCallable();
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, tc);
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(tc.getReturnedResult() == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackAlreadyDoneFutureTest() {
    TestCallable tc = new TestCallable();
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, tc);
    future.run();
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(tc.getReturnedResult() == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackExecutionExceptionAlreadyDoneTest() {
    RuntimeException failure = new RuntimeException();
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, new TestRuntimeFailureRunnable(failure));
    future.run();
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void addCallbackExecutionExceptionTest() {
    RuntimeException failure = new RuntimeException();
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, new TestRuntimeFailureRunnable(failure));
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test (expected = ExecutionException.class)
  public void getExecutionExceptionTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, tr, null);
    
    future.run();
    future.get();
  }
  
  @Test (expected = ExecutionException.class)
  public void getWithTimeoutExecutionExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestRunnable tr = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = new ListenableFutureTask<Object>(false, tr, null);
    
    future.run();
    future.get(100, TimeUnit.MILLISECONDS);
  }
  
  private class Factory implements FutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run) {
      return new ListenableFutureTask<Object>(false, run);
    }

    @Override
    public <T> RunnableFuture<T> make(Runnable run, T result) {
      return new ListenableFutureTask<T>(false, run, result);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return new ListenableFutureTask<T>(false, callable);
    }
  }
  
  private class TestExecutor implements Executor {
    public List<Runnable> providedRunnables = new LinkedList<Runnable>();
    
    @Override
    public void execute(Runnable command) {
      providedRunnables.add(command);
    }
  }
}

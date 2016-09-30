package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.ArrayList;
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
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.SuppressedStackRuntimeException;

@SuppressWarnings("javadoc")
public class ListenableFutureTaskTest extends ListenableRunnableFutureInterfaceTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setIgnoreExceptionHandler();
  }
  
  @Override
  protected ExecuteOnGetFutureFactory makeFutureFactory() {
    return new ListenableFutureTaskFactory();
  }
  
  protected <T> ListenableFutureTask<T> makeFutureTask(Runnable runnable, T result) {
    return new ListenableFutureTask<>(false, runnable, result);
  }
  
  protected <T> ListenableFutureTask<T> makeFutureTask(Callable<T> task) {
    return new ListenableFutureTask<>(false, task);
  }
  
  @Test
  public void getContainedRunnableTest() {
    TestRunnable tr = new TestRunnable();
    ListenableFutureTask<Object> f = makeFutureTask(tr, null);
    assertTrue(tr == ((RunnableContainer)f.getContainedCallable()).getContainedRunnable());
  }
  
  @Test
  public void addNullListenerTest() {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    
    future.addListener(null);
    // no exception should have been thrown
  }
  
  @Test
  public void listenerTest() {
    TestRunnable tr = new TestRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(tr, null);
    
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
  public void cancelRunsListenersTest() {
    TestRunnable tr = new TestRunnable();
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    future.addListener(tr);
    
    future.cancel(false);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void listenerExceptionAddBeforeRunTest() {
    TestRunnable listener = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    
    future.addListener(listener);
    future.run();
    
    assertTrue(listener.ranOnce());
  }
  
  @Test
  public void listenerExceptionAddAfterRunTest() {
    TestRunnable listener = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    
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
    ListenableFutureTask<Object> future = makeFutureTask(tc);
    TestFutureCallback tfc = new TestFutureCallback();
    future.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(tc.getReturnedResult() == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackExecutionExceptionTest() {
    RuntimeException failure = new SuppressedStackRuntimeException();
    ListenableFutureTask<Object> future = makeFutureTask(new TestRuntimeFailureRunnable(failure), null);
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
    
    ListenableFutureTask<Object> future = makeFutureTask(tr, null);
    
    future.run();
    future.get();
  }
  
  @Test (expected = ExecutionException.class)
  public void getWithTimeoutExecutionExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestRunnable tr = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(tr, null);
    
    future.run();
    future.get(100, TimeUnit.MILLISECONDS);
  }
  
  private class ListenableFutureTaskFactory implements ExecuteOnGetFutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run) {
      return new ListenableFutureTask<>(false, run);
    }

    @Override
    public <T> RunnableFuture<T> make(Runnable run, T result) {
      return new ListenableFutureTask<>(false, run, result);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return new ListenableFutureTask<>(false, callable);
    }

    @Override
    public ListenableFuture<?> makeCanceled() {
      ListenableFutureTask<?> lft = new ListenableFutureTask<>(false, DoNothingRunnable.instance());
      lft.cancel(false);
      return lft;
    }

    @Override
    public ListenableFuture<?> makeWithFailure(Exception e) {
      ListenableFutureTask<?> lft = new ListenableFutureTask<>(false, () -> { throw e; });
      lft.run();
      return lft;
    }

    @Override
    public <T> ListenableFuture<T> makeWithResult(T result) {
      ListenableFutureTask<T> lft = new ListenableFutureTask<>(false, () -> result);
      lft.run();
      return lft;
    }
  }
  
  private class TestExecutor implements Executor {
    public List<Runnable> providedRunnables = new ArrayList<>(2);
    
    @Override
    public void execute(Runnable command) {
      providedRunnables.add(command);
    }
  }
}

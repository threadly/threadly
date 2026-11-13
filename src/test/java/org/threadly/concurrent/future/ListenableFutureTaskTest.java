package org.threadly.concurrent.future;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StackSuppressedRuntimeException;

@SuppressWarnings("javadoc")
public class ListenableFutureTaskTest extends ListenableRunnableFutureInterfaceTest {
  @BeforeAll
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  @Override
  protected ListenableRunnableFutureFactory makeRunnableFutureFactory() {
    return new ListenableFutureTaskFactory();
  }
  
  protected <T> ListenableFutureTask<T> makeFutureTask(Runnable runnable, T result) {
    return new ListenableFutureTask<>(runnable, result);
  }
  
  protected <T> ListenableFutureTask<T> makeFutureTask(Callable<T> task) {
    return new ListenableFutureTask<>(task);
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
    
    future.listener(null);
    // no exception should have been thrown
  }
  
  @Test
  public void listenerTest() {
    TestRunnable tr = new TestRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(tr, null);
    
    assertEquals(0, future.listenerHelper.registeredListenerCount()); // empty to start
    
    TestRunnable listener = new TestRunnable();
    
    future.listener(listener);
    
    assertEquals(1, future.listenerHelper.registeredListenerCount()); // should now have once now that the runnable has not run yet
    
    future.run(); // this should call the listener
    
    assertTrue(listener.ranOnce()); // verify listener was called
    
    assertEquals(0, future.listenerHelper.registeredListenerCount()); // empty after listener calls
    
    TestRunnable postRunListener = new TestRunnable();
    
    future.listener(postRunListener);
    
    assertTrue(postRunListener.ranOnce()); // verify listener was called
    
    assertEquals(0, future.listenerHelper.registeredListenerCount()); // still empty after future ran
    
    // verify run on correct executor
    TestRunnable executorListener = new TestRunnable();
    TestExecutor executor = new TestExecutor();
    future.listener(executorListener, executor);
    
    assertEquals(1, executor.providedRunnables.size());
    assertTrue(executor.providedRunnables.get(0) == executorListener);
  }
  
  @Test
  public void cancelRunsListenersTest() {
    TestRunnable tr = new TestRunnable();
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    future.listener(tr);
    
    future.cancel(false);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void listenerExceptionAddBeforeRunTest() {
    TestRunnable listener = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    
    future.listener(listener);
    future.run();
    
    assertTrue(listener.ranOnce());
  }
  
  @Test
  public void listenerExceptionAddAfterRunTest() {
    TestRunnable listener = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    
    future.run();
    try {
      future.listener(listener);
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      // expected
    }
    
    assertTrue(listener.ranOnce());
  }
  
  @Test
  public void callbackTest() {
    TestCallable tc = new TestCallable();
    ListenableFutureTask<Object> future = makeFutureTask(tc);
    TestFutureCallback tfc = new TestFutureCallback();
    future.callback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(tc.getReturnedResult() == tfc.getLastResult());
  }
  
  @Test
  public void resultCallbackTest() {
    TestCallable tc = new TestCallable();
    ListenableFutureTask<Object> future = makeFutureTask(tc);
    TestFutureCallback tfc = new TestFutureCallback();
    future.resultCallback(tfc::handleResult);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(tc.getReturnedResult() == tfc.getLastResult());
  }
  
  @Test
  public void callbackExecutionExceptionTest() {
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFutureTask<Object> future = makeFutureTask(new TestRuntimeFailureRunnable(failure), null);
    TestFutureCallback tfc = new TestFutureCallback();
    future.callback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void failureCallbackExecutionExceptionTest() {
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFutureTask<Object> future = makeFutureTask(new TestRuntimeFailureRunnable(failure), null);
    TestFutureCallback tfc = new TestFutureCallback();
    future.failureCallback(tfc::handleFailure);
    
    assertEquals(0, tfc.getCallCount());
    
    future.run();
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void isCompletedExceptionallyTest() {
    TestRunnable tr = new TestRuntimeFailureRunnable();
    
    ListenableFutureTask<Object> future = makeFutureTask(tr, null);
    
    assertFalse(future.isCompletedExceptionally());
    future.run();
    assertTrue(future.isCompletedExceptionally());
  }
  
  @Test
  public void isDoneByCancelTest() {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    
    assertFalse(future.isDone());
    
    assertTrue(future.cancel(false));
    
    assertTrue(future.isDone());
    assertTrue(future.isCompletedExceptionally());
  }
  
  @Test
  @Override
  public void cancelFlatMappedAsyncFutureTest() {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    SettableListenableFuture<Void> asyncSLF = new SettableListenableFuture<>();
    ListenableFuture<Void> mappedLF = future.flatMap(asyncSLF);
      
    future.run();  // complete source future before cancel
    assertFalse(mappedLF.isDone());
    assertTrue(mappedLF.cancel(false)); // no interrupt needed, delegate future not started
    assertTrue(asyncSLF.isCancelled());
  }
  
  @Test
  @Override
  public void flatMapReturnNullFail() throws InterruptedException {
    try {
      ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
      ListenableFuture<Void> mappedLF = future.flatMap((o) -> null);
      future.run();
      mappedLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof NullPointerException);
      assertTrue(cause.getMessage().startsWith(InternalFutureUtils.NULL_FUTURE_MAP_RESULT_ERROR_PREFIX));
    }
  }
  
  @Test
  public void mapStackSizeTest() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    ListenableFutureInterfaceTest.mapStackDepthTest(future, future, 52, 37);
  }
  
  @Test
  public void mapFailureStackSize() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(() -> { throw new RuntimeException(); }, null);
    ListenableFutureInterfaceTest.mapFailureStackDepthTest(future, future, 52);
  }
  
  @Test
  public void flatMapStackSizeTest() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    ListenableFutureInterfaceTest.flatMapStackDepthTest(future, future, 72, 15);
  }
  
  @Test
  public void getRunningStackTraceTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    ListenableFutureTask<Object> future = makeFutureTask(btr, null);
    
    try {
      assertNull(future.getRunningStackTrace());
      
      sts.execute(future);
      btr.blockTillStarted();

      StackTraceElement[] stack = future.getRunningStackTrace();
      assertTrue(containsClass(stack, BlockingTestRunnable.class.getName()));
    } finally {
      btr.unblock();
      sts.shutdown();
    }
  }
  
  @Test
  public void getMappedRunningStackTraceTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    ListenableFutureTask<Object> futureTask = makeFutureTask(btr, null);
    ListenableFuture<Object> mappedFuture = futureTask.map((o) -> o).map((o) -> null);
    try {
      assertNull(mappedFuture.getRunningStackTrace());
      
      sts.execute(futureTask);
      btr.blockTillStarted();

      StackTraceElement[] stack = mappedFuture.getRunningStackTrace();
      assertTrue(containsClass(stack, BlockingTestRunnable.class.getName()));
    } finally {
      btr.unblock();
      sts.shutdown();
    }
  }
  
  @Test
  public void getFlatMappedRunningStackTraceTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    ListenableFutureTask<Object> futureTask = makeFutureTask(btr, null);
    ListenableFuture<Object> mappedFuture = 
        futureTask.flatMap((o) -> FutureUtils.immediateResultFuture(o))
                  .flatMap((o) -> FutureUtils.immediateResultFuture(null));
    try {
      assertNull(mappedFuture.getRunningStackTrace());
      
      sts.execute(futureTask);
      btr.blockTillStarted();

      StackTraceElement[] stack = mappedFuture.getRunningStackTrace();
      assertTrue(containsClass(stack, BlockingTestRunnable.class.getName()));
    } finally {
      btr.unblock();
      sts.shutdown();
    }
  }
  
  private static boolean containsClass(StackTraceElement[] stack, String className) {
    for (StackTraceElement e : stack) {
      if (className.equals(e.getClassName())) {
        return true;
      }
    }
    return false;
  }

  private class ListenableFutureTaskFactory implements ListenableRunnableFutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run) {
      return new ListenableFutureTask<>(run);
    }

    @Override
    public <T> RunnableFuture<T> make(Runnable run, T result) {
      return new ListenableFutureTask<>(run, result);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return new ListenableFutureTask<>(callable);
    }

    @Override
    public <T> ListenableFutureTask<T> makeNewCompletable() {
      return new ListenableFutureTask<>(DoNothingRunnable.instance());
    }

    @Override
    public <T> ListenableFutureTask<T> makeNewCompletable(Executor executor) {
      return new ListenableFutureTask<>(DoNothingRunnable.instance(), null, executor);
    }

    @Override
    public ListenableFutureTask<?> makeCanceledCompletable() {
      ListenableFutureTask<?> lft = new ListenableFutureTask<>(DoNothingRunnable.instance());
      lft.cancel(false);
      return lft;
    }

    @Override
    public ListenableFutureTask<Object> makeWithFailureCompletable(Exception e) {
      ListenableFutureTask<Object> lft = new ListenableFutureTask<>(() -> { throw e; });
      lft.run();
      return lft;
    }

    @Override
    public <T> ListenableFutureTask<T> makeWithResultCompletable(T result) {
      ListenableFutureTask<T> lft = new ListenableFutureTask<>(() -> result);
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

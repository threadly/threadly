package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorServiceWrapperTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  private static final int THREAD_COUNT = 1000;
  private static final int KEEP_ALIVE_TIME = 1000;
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new PriorityScheduledExecutorServiceWrapper(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void isTerminatedShortTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.isTerminatedShortTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void isTerminatedLongTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.isTerminatedLongTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.awaitTerminationTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, 
                                          ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.submitCallableTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void submitWithResultTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.submitWithResultTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void futureGetTimeoutFail() throws InterruptedException, 
                                            ExecutionException, 
                                            TimeoutException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.futureGetTimeoutFail(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = ExecutionException.class)
  public void futureGetExecutionFail() throws InterruptedException, 
                                              ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.futureGetExecutionFail(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void futureCancelTest() throws InterruptedException, 
                                        ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.futureCancelTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleRunnableTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleRunnableTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void scheduleRunnableFail() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      wrapper.schedule((Runnable)null, 10, TimeUnit.MILLISECONDS);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleCallableTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void scheduleCallableFail() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      wrapper.schedule((Callable<?>)null, 10, TimeUnit.MILLISECONDS);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleCallableCancelTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleAtFixedRateTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    executor.prestartAllCoreThreads();
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleAtFixedRateTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleAtFixedRateConcurrentTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    executor.prestartAllCoreThreads();
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleAtFixedRateConcurrentTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleAtFixedRateExceptionTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    executor.prestartAllCoreThreads();
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleAtFixedRateExceptionTest(wrapper);
      
      // fixed rate failure should have caused recurring task to be removed
      assertEquals(0, executor.highPriorityQueue.size());
      assertEquals(0, executor.lowPriorityQueue.size());
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleAtFixedRateFail() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleAtFixedRateFail(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    executor.prestartAllCoreThreads();
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleWithFixedDelayTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void scheduleWithFixedDelayFail() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleWithFixedDelayFail(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void invokeAllTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAllTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void invokeAllExceptionTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAllExceptionTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void invokeAllTimeoutTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAllTimeoutTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void invokeAllFail() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAllFail(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void invokeAnyTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAnyTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void invokeAnyTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAnyTimeoutTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void invokeAnyFail() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                             KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAnyFail(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void shutdownTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 200);
    try {
      PriorityScheduledExecutorServiceWrapper wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      
      wrapper.shutdownNow();
      
      assertTrue(wrapper.isShutdown());
      assertTrue(executor.isShutdown());
      
      try {
        wrapper.execute(new TestRunnable());
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void listenableFutureTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 200);
    try {
      PriorityScheduledExecutorServiceWrapper wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      TestRunnable futureListener = new TestRunnable();
      ListenableFuture<?> future = wrapper.submit(new TestRunnable());
      future.addListener(futureListener);
      
      futureListener.blockTillFinished(); // throws exception if never called
    } finally {
      executor.shutdownNow();
    }
  }
}

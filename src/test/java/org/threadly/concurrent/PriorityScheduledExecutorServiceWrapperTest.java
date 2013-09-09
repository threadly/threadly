package org.threadly.concurrent;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorServiceWrapperTest {
  private static final int THREAD_COUNT = 1000;
  private static final int KEEP_ALIVE_TIME = 200;
  
  static {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // ignored
      }
    });
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new PriorityScheduledExecutorServiceWrapper(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void isTerminatedShortTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleRunnableTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleCallableTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleCallableCancelTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAllTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void invokeAllFail() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.invokeAnyTest(wrapper);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void invokeAnyFail() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 200);
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
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 200);
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

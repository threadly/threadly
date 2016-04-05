package org.threadly.concurrent.wrapper;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;

@SuppressWarnings("javadoc")
public class PrioritySchedulerDefaultPriorityWrapperTest {
  private static PriorityScheduler scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduler(2);
  }
  
  @AfterClass
  public static void cleanupClass() {
    scheduler.shutdown();
    scheduler = null;
  }
  
  @Test
  public void constructorTest() {
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(scheduler, TaskPriority.Low);
    assertTrue(psw.scheduler == scheduler);
    assertEquals(TaskPriority.Low, psw.defaultPriority);
    psw = new PrioritySchedulerDefaultPriorityWrapper(scheduler, TaskPriority.High);
    assertEquals(TaskPriority.High, psw.defaultPriority);
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new PrioritySchedulerDefaultPriorityWrapper(null, TaskPriority.High);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PrioritySchedulerDefaultPriorityWrapper(scheduler, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void isShutdownTest() {
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(scheduler, TaskPriority.Low);
    assertEquals(scheduler.isShutdown(), psw.isShutdown());
    
    TestPriorityScheduler tps = new TestPriorityScheduler();
    psw = new PrioritySchedulerDefaultPriorityWrapper(tps, TaskPriority.Low);
    psw.isShutdown();
    
    assertTrue(tps.isShutdownCalled);
  }
  
  @Test
  public void executeTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.execute(DoNothingRunnable.instance());
    assertTrue(testScheduler.executeCalled);
    
    // reset and try with priority
    testScheduler.executeCalled = false;
    psw.execute(DoNothingRunnable.instance(), TaskPriority.High);
    assertTrue(testScheduler.executeCalled);
  }
  
  @Test
  public void scheduleTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.schedule(DoNothingRunnable.instance(), 10);
    assertTrue(testScheduler.scheduleCalled);
    
    // reset and try with priority
    testScheduler.scheduleCalled = false;
    psw.schedule(DoNothingRunnable.instance(), 10, TaskPriority.High);
    assertTrue(testScheduler.scheduleCalled);
  }
  
  @Test
  public void submitRunnableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.submit(DoNothingRunnable.instance());
    assertTrue(testScheduler.submitRunnableCalled);
    
    // reset and try with priority
    testScheduler.submitRunnableCalled = false;
    psw.submit(DoNothingRunnable.instance(), TaskPriority.High);
    assertTrue(testScheduler.submitRunnableCalled);
  }
  
  @Test
  public void submitRunnableWithResultTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.submit(DoNothingRunnable.instance(), new Object());
    assertTrue(testScheduler.submitRunnableResultCalled);
    
    // reset and try with priority
    testScheduler.submitRunnableResultCalled = false;
    psw.submit(DoNothingRunnable.instance(), new Object(), TaskPriority.High);
    assertTrue(testScheduler.submitRunnableResultCalled);
  }
  
  @Test
  public void submitCallableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.submit(new TestCallable());
    assertTrue(testScheduler.submitCallableCalled);
    
    // reset and try with priority
    testScheduler.submitCallableCalled = false;
    psw.submit(new TestCallable(), TaskPriority.High);
    assertTrue(testScheduler.submitCallableCalled);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.submitScheduled(DoNothingRunnable.instance(), 10);
    assertTrue(testScheduler.submitScheduledRunnableCalled);
    
    // reset and try with priority
    testScheduler.submitScheduledRunnableCalled = false;
    psw.submitScheduled(DoNothingRunnable.instance(), 10, TaskPriority.High);
    assertTrue(testScheduler.submitScheduledRunnableCalled);
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.submitScheduled(DoNothingRunnable.instance(), new Object(), 10);
    assertTrue(testScheduler.submitScheduledRunnableResultCalled);
    
    // reset and try with priority
    testScheduler.submitScheduledRunnableResultCalled = false;
    psw.submitScheduled(DoNothingRunnable.instance(), new Object(), 10, TaskPriority.High);
    assertTrue(testScheduler.submitScheduledRunnableResultCalled);
  }
  
  @Test
  public void submitScheduledCallableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.submitScheduled(new TestCallable(), 10);
    assertTrue(testScheduler.submitScheduledCallableCalled);
    
    // reset and try with priority
    testScheduler.submitScheduledCallableCalled = false;
    psw.submitScheduled(new TestCallable(), 10, TaskPriority.High);
    assertTrue(testScheduler.submitScheduledCallableCalled);
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.scheduleWithFixedDelay(DoNothingRunnable.instance(), 10, 10);
    assertTrue(testScheduler.scheduleWithFixedDelayCalled);
    
    // reset and try with priority
    testScheduler.scheduleWithFixedDelayCalled = false;
    psw.scheduleWithFixedDelay(DoNothingRunnable.instance(), 10, 10, TaskPriority.High);
    assertTrue(testScheduler.scheduleWithFixedDelayCalled);
  }
  
  @Test
  public void scheduleAtFixedRateTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    psw.scheduleAtFixedRate(DoNothingRunnable.instance(), 10, 10);
    assertTrue(testScheduler.scheduleAtFixedRateCalled);
    
    // reset and try with priority
    testScheduler.scheduleAtFixedRateCalled = false;
    psw.scheduleAtFixedRate(DoNothingRunnable.instance(), 10, 10, TaskPriority.High);
    assertTrue(testScheduler.scheduleAtFixedRateCalled);
  }
  
  @Test
  public void removeRunnableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    
    psw.remove(DoNothingRunnable.instance());
    
    assertTrue(testScheduler.removeRunnableCalled);
  }
  
  @Test
  public void removeCallableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerDefaultPriorityWrapper psw = 
        new PrioritySchedulerDefaultPriorityWrapper(testScheduler, TaskPriority.Low);
    
    psw.remove(new TestCallable());
    
    assertTrue(testScheduler.removeCallableCalled);
  }
  
  // TODO - this may be good to move to something like mockito
  protected static class TestPriorityScheduler implements PrioritySchedulerService {
    protected boolean isShutdownCalled = false;
    protected boolean executeCalled = false;
    protected boolean scheduleCalled = false;
    protected boolean submitRunnableCalled = false;
    protected boolean submitRunnableResultCalled = false;
    protected boolean submitCallableCalled = false;
    protected boolean submitScheduledRunnableCalled = false;
    protected boolean submitScheduledRunnableResultCalled = false;
    protected boolean submitScheduledCallableCalled = false;
    protected boolean scheduleWithFixedDelayCalled = false;
    protected boolean scheduleAtFixedRateCalled = false;
    protected boolean removeRunnableCalled = false;
    protected boolean removeCallableCalled = false;

    @Override
    public boolean isShutdown() {
      isShutdownCalled = true;
      return false;
    }

    @Override
    public void execute(Runnable task, TaskPriority priority) {
      executeCalled = true;
    }

    @Override
    public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
      submitRunnableCalled = true;
      return FutureUtils.immediateFailureFuture(new UnsupportedOperationException());
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
      submitRunnableResultCalled = true;
      return FutureUtils.immediateFailureFuture(new UnsupportedOperationException());
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
      submitCallableCalled = true;
      return FutureUtils.immediateFailureFuture(new UnsupportedOperationException());
    }

    @Override
    public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
      scheduleCalled = true;
    }

    @Override
    public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, TaskPriority priority) {
      submitScheduledRunnableCalled = true;
      return FutureUtils.immediateFailureFuture(new UnsupportedOperationException());
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                   TaskPriority priority) {
      submitScheduledRunnableResultCalled = true;
      return FutureUtils.immediateFailureFuture(new UnsupportedOperationException());
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                   TaskPriority priority) {
      submitScheduledCallableCalled = true;
      return FutureUtils.immediateFailureFuture(new UnsupportedOperationException());
    }

    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                       TaskPriority priority) {
      scheduleWithFixedDelayCalled = true;
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                    TaskPriority priority) {
      scheduleAtFixedRateCalled = true;
    }

    @Override
    public boolean remove(Runnable task) {
      removeRunnableCalled = true;
      return false;
    }

    @Override
    public boolean remove(Callable<?> task) {
      removeCallableCalled = true;
      return false;
    }

    @Override
    public TaskPriority getDefaultPriority() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxWaitForLowPriority() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getActiveTaskCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getQueuedTaskCount() {
      throw new UnsupportedOperationException();
    }
    
    // NO OPERATIONS WITHOUT PRIORITY SHOULD BE CALLED
    @Override
    public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void schedule(Runnable task, long delayInMs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
      throw new UnsupportedOperationException();
    }
  }
}

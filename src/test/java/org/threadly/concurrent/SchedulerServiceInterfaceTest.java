package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.Callable;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public abstract class SchedulerServiceInterfaceTest extends SubmitterSchedulerInterfaceTest {
  protected abstract SchedulerServiceFactory getSchedulerServiceFactory();

  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return getSchedulerServiceFactory();
  }
  
  @Test
  public void removeRunnableTest() {
    SchedulerServiceFactory factory = getSchedulerServiceFactory();
    
    SchedulerService scheduler = factory.makeSchedulerService(1, false);
    BlockingTestRunnable btr1 = new BlockingTestRunnable();
    BlockingTestRunnable btr2 = new BlockingTestRunnable();
    try {
      assertFalse(scheduler.remove(btr1));
      assertFalse(scheduler.remove((Runnable)null));
      
      scheduler.execute(btr1);
      scheduler.execute(btr2);
      
      TestRunnable tr = new TestRunnable();
      
      assertFalse(scheduler.remove(tr));

      scheduler.execute(tr);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.submit(tr);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.submit(tr, new Object());
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.schedule(tr, 1000 * 10);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.submitScheduled(tr, 1000 * 10);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.submitScheduled(tr, new Object(), 1000 * 10);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.scheduleWithFixedDelay(tr, 0, 1000 * 10);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
    } finally {
      btr1.unblock();
      btr2.unblock();
      factory.shutdown();
    }
  }

  @Test
  public void removeCallableTest() {
    SchedulerServiceFactory factory = getSchedulerServiceFactory();
    
    SchedulerService scheduler = factory.makeSchedulerService(1, false);
    BlockingTestRunnable btr1 = new BlockingTestRunnable();
    BlockingTestRunnable btr2 = new BlockingTestRunnable();
    try {
      assertFalse(scheduler.remove(btr1));
      
      scheduler.execute(btr1);
      scheduler.execute(btr2);
      
      TestCallable tc = new TestCallable();
      
      assertFalse(scheduler.remove(tc));
      assertFalse(scheduler.remove((Callable<?>)null));
      
      scheduler.submit(tc);
      assertTrue(scheduler.remove(tc));
      assertFalse(scheduler.remove(tc));
      
      scheduler.submitScheduled(tc, DELAY_TIME);
      assertTrue(scheduler.remove(tc));
      assertFalse(scheduler.remove(tc));
    } finally {
      btr1.unblock();
      btr2.unblock();
      factory.shutdown();
    }
  }
  
  @Test
  public void getActiveTaskCountTest() {
    SchedulerServiceFactory factory = getSchedulerServiceFactory();
    final SchedulerService scheduler = factory.makeSchedulerService(1, false);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getActiveTaskCount());
      
      BlockingTestRunnable btr = new BlockingTestRunnable();
      scheduler.execute(btr);
      btr.blockTillStarted();
      
      assertEquals(1, scheduler.getActiveTaskCount());
      
      btr.unblock();
      
      new TestCondition(() -> scheduler.getActiveTaskCount() == 0).blockTillTrue();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getQueuedTaskCountTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    SchedulerServiceFactory factory = getSchedulerServiceFactory();
    final SchedulerService scheduler = factory.makeSchedulerService(1, false);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getQueuedTaskCount());
      
      scheduler.execute(btr);
      btr.blockTillStarted();
      
      assertEquals(0, scheduler.getQueuedTaskCount());
      
      scheduler.execute(DoNothingRunnable.instance());
      assertEquals(1, scheduler.getQueuedTaskCount());
    } finally {
      btr.unblock();
      factory.shutdown();
    }
  }
  
  @Test
  public void getWaitingForExecutionTaskCountTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    SchedulerServiceFactory factory = getSchedulerServiceFactory();
    final SchedulerService scheduler = factory.makeSchedulerService(1, false);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getWaitingForExecutionTaskCount());
      
      scheduler.execute(btr);
      btr.blockTillStarted();
      
      assertEquals(0, scheduler.getWaitingForExecutionTaskCount());
      
      scheduler.execute(DoNothingRunnable.instance());
      assertEquals(1, scheduler.getWaitingForExecutionTaskCount());
      scheduler.schedule(DoNothingRunnable.instance(), 1);
      TestUtils.blockTillClockAdvances();
      assertEquals(2, scheduler.getWaitingForExecutionTaskCount());
      scheduler.schedule(DoNothingRunnable.instance(), 600_000);
      assertEquals(2, scheduler.getWaitingForExecutionTaskCount());
    } finally {
      btr.unblock();
      factory.shutdown();
    }
  }
  
  public interface SchedulerServiceFactory extends SubmitterSchedulerFactory {
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable);
    
    @Override
    public default SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }
  }
}

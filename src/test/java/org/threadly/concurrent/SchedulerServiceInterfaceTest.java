package org.threadly.concurrent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.threadly.TestConstants.SCHEDULE_DELAY;

import java.util.concurrent.Callable;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;

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
    
    SchedulerServiceInterface scheduler = factory.makeSchedulerService(1, false);
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
      
      scheduler.schedule(tr, SCHEDULE_DELAY);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.submitScheduled(tr, SCHEDULE_DELAY);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.submitScheduled(tr, new Object(), SCHEDULE_DELAY);
      assertTrue(scheduler.remove(tr));
      assertFalse(scheduler.remove(tr));
      
      scheduler.scheduleWithFixedDelay(tr, 0, SCHEDULE_DELAY);
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
    
    SchedulerServiceInterface scheduler = factory.makeSchedulerService(1, false);
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
      
      scheduler.submitScheduled(tc, SCHEDULE_DELAY);
      assertTrue(scheduler.remove(tc));
      assertFalse(scheduler.remove(tc));
    } finally {
      btr1.unblock();
      btr2.unblock();
      factory.shutdown();
    }
  }
  
  public interface SchedulerServiceFactory extends SubmitterSchedulerFactory {
    public SchedulerServiceInterface makeSchedulerService(int poolSize, 
                                                          boolean prestartIfAvailable);
  }
}

package org.threadly.concurrent.wrapper;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class SchedulerExecutorDelegatorTest extends SubmitterSchedulerInterfaceTest {
  private TestableScheduler executor;
  private TestableScheduler scheduler;
  private SchedulerExecutorDelegator delegator;
  
  @Before
  public void setup() {
    executor = new TestableScheduler();
    scheduler = new TestableScheduler();
    delegator = new SchedulerExecutorDelegator(executor, scheduler);
  }
  
  @After
  public void cleanup() {
    executor = null;
    scheduler = null;
    delegator = null;
  }

  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new DelegatorFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return false;
  }
  
  @Test
  public void executeDelegateTest() {
    delegator.execute(DoNothingRunnable.instance());
    delegator.submit(DoNothingRunnable.instance());
    assertEquals(2, executor.tick());
    assertEquals(0, scheduler.tick());
  }
  
  @Test
  public void scheduleDelegateTest() {
    delegator.schedule(DoNothingRunnable.instance(), 10);
    delegator.submitScheduled(DoNothingRunnable.instance(), 10);
    delegator.scheduleAtFixedRate(DoNothingRunnable.instance(), 10, 100);
    delegator.scheduleWithFixedDelay(DoNothingRunnable.instance(), 10, 100);
    assertEquals(0, executor.advance(10));
    assertEquals(4, scheduler.advance(10));
  }

  protected static class DelegatorFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory;
    
    public DelegatorFactory() {
      schedulerFactory = new PrioritySchedulerFactory();
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      return new SchedulerExecutorDelegator(scheduler, scheduler);
    }
  }
}

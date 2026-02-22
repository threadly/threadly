package org.threadly.concurrent.wrapper.priority;

import static org.junit.jupiter.api.Assertions.*;

import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.TaskPriority;
import org.threadly.test.concurrent.TestableScheduler;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class PriorityDelegatingSchedulerTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new PriorityDelegatingSchedulerFactory();
  }

  @Override
  protected boolean isSingleThreaded() {
    return false;
  }
  
  @Test
  public void schedulerPriorityTest() {
    TestableScheduler highScheduler = new TestableScheduler();
    TestableScheduler lowScheduler = new TestableScheduler();
    TestableScheduler starvableScheduler = new TestableScheduler();
    PriorityDelegatingScheduler delegatingScheduler = 
        new PriorityDelegatingScheduler(highScheduler, lowScheduler, starvableScheduler, 
                                        TaskPriority.High);
    
    assertTrue(highScheduler == delegatingScheduler.scheduler(null));
    assertTrue(highScheduler == delegatingScheduler.scheduler(TaskPriority.High));
    assertTrue(lowScheduler == delegatingScheduler.scheduler(TaskPriority.Low));
    assertTrue(starvableScheduler == delegatingScheduler.scheduler(TaskPriority.Starvable));
  }
  
  @Test
  public void schedulerPriorityWithoutStarvableSchedulerTest() {
    TestableScheduler highScheduler = new TestableScheduler();
    TestableScheduler lowScheduler = new TestableScheduler();
    PriorityDelegatingScheduler delegatingScheduler = 
        new PriorityDelegatingScheduler(highScheduler, lowScheduler, null, TaskPriority.Starvable);
    
    assertTrue(highScheduler == delegatingScheduler.scheduler(TaskPriority.High));
    assertTrue(lowScheduler == delegatingScheduler.scheduler(TaskPriority.Low));
    assertTrue(lowScheduler == delegatingScheduler.scheduler(TaskPriority.Starvable));
    assertTrue(lowScheduler == delegatingScheduler.scheduler(null));
  }
  
  @Test
  public void executeHighPriorityTest() {
    TestableScheduler highScheduler = new TestableScheduler();
    TestableScheduler lowScheduler = new TestableScheduler();
    PriorityDelegatingScheduler delegatingScheduler = 
        new PriorityDelegatingScheduler(highScheduler, lowScheduler, null, TaskPriority.Starvable);
    
    delegatingScheduler.execute(DoNothingRunnable.instance(), TaskPriority.High);
    
    assertEquals(1, highScheduler.tick());
    assertEquals(0, lowScheduler.tick());
  }
  
  @Test
  public void submitHighPriorityTest() {
    TestableScheduler highScheduler = new TestableScheduler();
    TestableScheduler lowScheduler = new TestableScheduler();
    PriorityDelegatingScheduler delegatingScheduler = 
        new PriorityDelegatingScheduler(highScheduler, lowScheduler, null, TaskPriority.Starvable);
    
    delegatingScheduler.submit(DoNothingRunnable.instance(), TaskPriority.High);
    
    assertEquals(1, highScheduler.tick());
    assertEquals(0, lowScheduler.tick());
  }
  
  @Test
  public void submitScheduledHighPriorityTest() {
    TestableScheduler highScheduler = new TestableScheduler();
    TestableScheduler lowScheduler = new TestableScheduler();
    PriorityDelegatingScheduler delegatingScheduler = 
        new PriorityDelegatingScheduler(highScheduler, lowScheduler, null, TaskPriority.Starvable);
    
    delegatingScheduler.submitScheduled(DoNothingRunnable.instance(), 100, TaskPriority.High);
    
    assertEquals(1, highScheduler.advance(100));
    assertEquals(0, lowScheduler.advance(100));
  }
  
  protected static class PriorityDelegatingSchedulerFactory implements SchedulerServiceFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      SchedulerService scheduler = schedulerFactory.makeSchedulerService(poolSize, 
                                                                         prestartIfAvailable);
      return new PriorityDelegatingScheduler(scheduler, scheduler, null, TaskPriority.High);
    }
  }
  
}

package org.threadly.test.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.StringUtils;

@SuppressWarnings({"javadoc", "deprecation"})
public class TestablePrioritySchedulerTest extends TestableSchedulerTest {
  protected TestablePriorityScheduler priorityScheduler;
  
  @Before
  @Override
  public void setup() {
    priorityScheduler = new TestablePriorityScheduler();
    scheduler = priorityScheduler;
  }
  
  @After
  @Override
  public void cleanup() {
    super.cleanup();
    
    priorityScheduler = null;
  }
  
  @Test
  public void executeWithPriorityTest() {
    TestRunnable tr = new TestRunnable();
    priorityScheduler.execute(tr, TaskPriority.High);
    
    assertEquals(1, priorityScheduler.tick());
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void submitRunnableWithPriorityTest() {
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> f = priorityScheduler.submit(tr, TaskPriority.High);
    
    assertEquals(1, priorityScheduler.tick());
    
    assertTrue(tr.ranOnce());
    assertTrue(f.isDone());
  }
  
  @Test
  public void submitRunnableWithResultAndPriorityTest() throws InterruptedException, 
                                                               ExecutionException {
    String result = StringUtils.makeRandomString(5);
    TestRunnable tr = new TestRunnable();
    ListenableFuture<String> f = priorityScheduler.submit(tr, result, TaskPriority.High);
    
    assertEquals(1, priorityScheduler.tick());
    
    assertTrue(tr.ranOnce());
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void submitCallableWithPriorityTest() throws InterruptedException, ExecutionException {
    final String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> f = priorityScheduler.submit(new Callable<String>() {
      @Override
      public String call() {
        return result;
      }
    }, TaskPriority.High);
    
    assertEquals(1, priorityScheduler.tick());
    
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void scheduledRunnableWithPriorityTest() {
    final int delay = 100;
    TestRunnable tr = new TestRunnable();
    priorityScheduler.schedule(tr, delay, TaskPriority.High);
    
    assertEquals(1, priorityScheduler.advance(delay));
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void submitScheduledRunnableWithPriorityTest() {
    final int delay = 100;
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> f = priorityScheduler.submitScheduled(tr, delay, TaskPriority.High);
    
    assertEquals(1, priorityScheduler.advance(delay));
    
    assertTrue(tr.ranOnce());
    assertTrue(f.isDone());
  }
  
  @Test
  public void submitScheduledRunnableWithResultAndPriorityTest() throws InterruptedException, 
                                                                        ExecutionException {
    final int delay = 100;
    String result = StringUtils.makeRandomString(5);
    TestRunnable tr = new TestRunnable();
    ListenableFuture<String> f = priorityScheduler.submitScheduled(tr, result, delay, 
                                                                   TaskPriority.High);

    assertEquals(1, priorityScheduler.advance(delay));
    
    assertTrue(tr.ranOnce());
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void submitScheduledCallableWithPriorityTest() throws InterruptedException, 
                                                               ExecutionException {
    final int delay = 100;
    final String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> f = priorityScheduler.submitScheduled(new Callable<String>() {
      @Override
      public String call() {
        return result;
      }
    }, delay, TaskPriority.High);

    assertEquals(1, priorityScheduler.advance(delay));
    
    assertTrue(f.isDone());
    assertTrue(result == f.get());
  }
  
  @Test
  public void scheduleWithFixedDelayWithPriorityTest() {
    recurringTest(true);
  }
  
  @Test
  public void scheduleAtFixedRateWithPriorityTest() {
    recurringTest(false);
  }
  
  private void recurringTest(boolean fixedDelay) {
    final int delay = 100;
    TestRunnable tr = new TestRunnable();
    if (fixedDelay) {
      priorityScheduler.scheduleWithFixedDelay(tr, delay, delay, TaskPriority.High);
    } else {
      priorityScheduler.scheduleAtFixedRate(tr, delay, delay, TaskPriority.High);
    }
    
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(1, priorityScheduler.advance(delay));
    }
    
    assertEquals(TEST_QTY, tr.getRunCount());
  }
  
  @Test
  public void getDefaultPriorityTest() {
    assertEquals(TaskPriority.High, priorityScheduler.getDefaultPriority());
  }
}

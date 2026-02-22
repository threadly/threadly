package org.threadly.concurrent.wrapper.interceptor;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTaskInterceptorTest extends SchedulerServiceTaskInterceptorTest {
  protected PrioritySchedulerTaskInterceptor priorityInterceptor;
  @BeforeEach
  @Override
  public void setup() {
    scheduler = new TestableScheduler();
    priorityInterceptor = new TestPrioritySchedulerInterceptor(scheduler);
    executorInterceptor = submitterSchedulerInterceptor = priorityInterceptor;
    testInterceptor = (TestInterceptor)executorInterceptor;
    interceptedTasks = new ArrayList<>(1);
    submitSchedulerTaskInterceptorLamba = new PrioritySchedulerTaskInterceptor(scheduler, (r1, b1) -> { 
      interceptedTasks.add(r1);
      
      return DoNothingRunnable.instance();
    });  
    tr = new TestRunnable();
  }
  
  @AfterEach
  @Override
  public void cleanup() {
    priorityInterceptor = null;
    super.cleanup();
  }
  
  @Test
  public void interceptExecuteWithPriorityTest() {
    priorityInterceptor.execute(tr, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }
  
  @Test
  public void interceptSubmitRunnableWithPriorityTest() {
    ListenableFuture<?> f = priorityInterceptor.submit(tr, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
  }
  
  @Test
  public void interceptSubmitRunnableWithResultWithPriorityTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFuture<?> f = priorityInterceptor.submit(tr, result, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
    assertTrue(f.get() == result);
  }
  
  @Test
  public void interceptSubmitCallableWithPriorityTest() {
    ListenableFuture<?> f = priorityInterceptor.submit(new TestCallable(), TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(testInterceptor.getInterceptedTasks().get(0) instanceof ListenableFutureTask);
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertFalse(f.isDone());
  }

  @Test
  public void interceptScheduleWithPriorityTest() {
    priorityInterceptor.schedule(tr, DELAY_TIME, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }

  @Test
  public void interceptSubmitScheduledRunnableWithPriorityTest() {
    ListenableFuture<?> f = priorityInterceptor.submitScheduled(tr, DELAY_TIME, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
  }

  @Test
  public void interceptSubmitScheduledRunnableWithResultWithPriorityTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFuture<Object> f = priorityInterceptor.submitScheduled(tr, result, DELAY_TIME, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
    assertTrue(f.get() == result);
  }

  @Test
  public void interceptSubmitScheduledCallableWithPriorityTest() {
    ListenableFuture<?> f = priorityInterceptor.submitScheduled(new TestCallable(), DELAY_TIME, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(testInterceptor.getInterceptedTasks().get(0) instanceof ListenableFutureTask);
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertFalse(f.isDone());
  }

  @Test
  public void interceptScheduleWithFixedDelayWithPriorityTest() {
    priorityInterceptor.scheduleWithFixedDelay(tr, DELAY_TIME, DELAY_TIME, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }

  @Test
  public void interceptScheduleAtFixedRateWithPriorityTest() {
    priorityInterceptor.scheduleAtFixedRate(tr, DELAY_TIME, DELAY_TIME, TaskPriority.Low);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }
  
  @Override
  @Test
  public void defaultConstructorFail() {
      assertThrows(RuntimeException.class, () -> {
      PrioritySchedulerTaskInterceptor psti = new PrioritySchedulerTaskInterceptor(scheduler);
      psti.schedule(DoNothingRunnable.instance(), 10, TaskPriority.Low);
      });
  }

  @Override
  @Test
  public void nullConstructorFail() {
      assertThrows(RuntimeException.class, () -> {
      @SuppressWarnings("unused")
      PrioritySchedulerTaskInterceptor psti = new PrioritySchedulerTaskInterceptor(scheduler, null);
      });
  }

  private static class TestPrioritySchedulerInterceptor extends PrioritySchedulerTaskInterceptor 
                                                        implements TestInterceptor {
    private final List<Runnable> interceptedTasks;
    
    public TestPrioritySchedulerInterceptor(PrioritySchedulerService parentScheduler) {
      super(parentScheduler);
      
      interceptedTasks = new ArrayList<>(1);
    }

    @Override
    public List<Runnable> getInterceptedTasks() {
      return interceptedTasks;
    }

    @Override
    public Runnable wrapTask(Runnable task, boolean recurring) {
      interceptedTasks.add(task);
      
      return DoNothingRunnable.instance();
    }
  }
}

package org.threadly.concurrent.wrapper.interceptor;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerTaskInterceptorTest extends ExecutorTaskInterceptorTest {
  protected SubmitterSchedulerTaskInterceptor submitterSchedulerInterceptor;
  protected SubmitterSchedulerTaskInterceptor submitSchedulerTaskInterceptorLamba;
  protected List<Runnable> interceptedTasks;
  
  @Before
  @Override
  public void setup() {
    scheduler = new TestableScheduler();
    submitterSchedulerInterceptor = new TestSubmitterSchedulerInterceptor(scheduler);
    executorInterceptor = submitterSchedulerInterceptor;
    testInterceptor = (TestInterceptor)executorInterceptor;
    interceptedTasks = new ArrayList<Runnable>(1);
    submitSchedulerTaskInterceptorLamba = new SubmitterSchedulerTaskInterceptor(scheduler, (r1, b1) -> { 
      interceptedTasks.add(r1);
      
      return DoNothingRunnable.instance();
    });
    tr = new TestRunnable();
  }
  
  @After
  @Override
  public void cleanup() {
    submitterSchedulerInterceptor = null;
    super.cleanup();
  }

  @Test
  public void interceptScheduleTest() {
    submitterSchedulerInterceptor.schedule(tr, DELAY_TIME);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }
  
  @Test
  public void interceptScheduleLambdaTest() {
    submitSchedulerTaskInterceptorLamba.schedule(tr, DELAY_TIME);
    
    assertEquals(1, interceptedTasks.size());
    assertTrue(tr == interceptedTasks.get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }

  @Test
  public void interceptSubmitScheduledRunnableTest() {
    ListenableFuture<?> f = submitterSchedulerInterceptor.submitScheduled(tr, DELAY_TIME);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
  }

  @Test
  public void interceptSubmitScheduledRunnableWithResultTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFuture<Object> f = submitterSchedulerInterceptor.submitScheduled(tr, result, DELAY_TIME);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
    assertTrue(f.get() == result);
  }

  @Test
  public void interceptSubmitScheduledCallableTest() {
    ListenableFuture<?> f = submitterSchedulerInterceptor.submitScheduled(new TestCallable(), DELAY_TIME);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(testInterceptor.getInterceptedTasks().get(0) instanceof ListenableFutureTask);
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertFalse(f.isDone());
  }

  @Test
  public void interceptScheduleWithFixedDelayTest() {
    submitterSchedulerInterceptor.scheduleWithFixedDelay(tr, DELAY_TIME, DELAY_TIME);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }

  @Test
  public void interceptScheduleAtFixedRateTest() {
    submitterSchedulerInterceptor.scheduleAtFixedRate(tr, DELAY_TIME, DELAY_TIME);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.advance(DELAY_TIME));  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }

  private static class TestSubmitterSchedulerInterceptor extends SubmitterSchedulerTaskInterceptor 
                                                         implements TestInterceptor {
    private final List<Runnable> interceptedTasks;
    
    public TestSubmitterSchedulerInterceptor(SubmitterScheduler parentScheduler) {
      super(parentScheduler);
      
      interceptedTasks = new ArrayList<Runnable>(1);
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

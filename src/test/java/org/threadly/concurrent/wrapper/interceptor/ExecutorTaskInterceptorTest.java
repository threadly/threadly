package org.threadly.concurrent.wrapper.interceptor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class ExecutorTaskInterceptorTest {
  protected TestableScheduler scheduler;
  protected ExecutorTaskInterceptor executorInterceptor;
  protected TestInterceptor testInterceptor;
  protected TestRunnable tr;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    executorInterceptor = new TestExecutorInterceptor(scheduler);
    testInterceptor = (TestInterceptor)executorInterceptor;
    tr = new TestRunnable();
  }
  
  @After
  public void cleanup() {
    scheduler = null;
    executorInterceptor = null;
    testInterceptor = null;
    tr = null;
  }
  
  @Test
  public void interceptExecuteTest() {
    executorInterceptor.execute(tr);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }
  
  @Test
  public void interceptSubmitRunnableTest() {
    ListenableFuture<?> f = executorInterceptor.submit(tr);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
  }
  
  @Test
  public void interceptSubmitRunnableLambdaTest() {
    List<Runnable> interceptedTasks = new ArrayList<Runnable>(1);
    
    ExecutorTaskInterceptor executorInterceptorLambda = new ExecutorTaskInterceptor(scheduler, (r1) -> { 
      interceptedTasks.add(r1);
      
      return DoNothingRunnable.instance();
    });
    executorInterceptorLambda.execute(tr);
    
    assertEquals(1, interceptedTasks.size());
    assertTrue(tr == interceptedTasks.get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
  }
  
  @Test
  public void interceptSubmitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFuture<?> f = executorInterceptor.submit(tr, result);

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(tr == testInterceptor.getInterceptedTasks().get(0));
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertEquals(0, tr.getRunCount());  // should have been replaced and not run
    assertTrue(f.isDone());
    assertTrue(f.get() == result);
  }
  
  @Test
  public void interceptSubmitCallableTest() {
    ListenableFuture<?> f = executorInterceptor.submit(new TestCallable());

    assertEquals(1, testInterceptor.getInterceptedTasks().size());
    assertTrue(testInterceptor.getInterceptedTasks().get(0) instanceof ListenableFutureTask);
    assertEquals(1, scheduler.tick());  // replaced task should run
    assertFalse(f.isDone());
  }
  
  protected interface TestInterceptor {
    public List<Runnable> getInterceptedTasks();
  }

  private static class TestExecutorInterceptor extends ExecutorTaskInterceptor 
                                               implements TestInterceptor {
    private final List<Runnable> interceptedTasks;
    
    public TestExecutorInterceptor(Executor parentExecutor) {
      super(parentExecutor);
      
      interceptedTasks = new ArrayList<Runnable>(1);
    }

    @Override
    public List<Runnable> getInterceptedTasks() {
      return interceptedTasks;
    }

    @Override
    public Runnable wrapTask(Runnable task) {
      interceptedTasks.add(task);
      
      return DoNothingRunnable.instance();
    }
  }
}

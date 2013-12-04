package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest.SubmitterExecutorFactory;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecutorWrapperTest {
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ExecutorWrapper(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void executeTest() {
    TestExecutor te = new TestExecutor();
    ExecutorWrapper ew = new ExecutorWrapper(te);
    Runnable r = new TestRunnable();
    
    ew.execute(r);
    
    assertTrue(te.lastCommand == r);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    TestExecutor te = new TestExecutor();
    ExecutorWrapper ew = new ExecutorWrapper(te);
    
    ew.execute(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    ExecutorWrapperFactory ef = new ExecutorWrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableTest(ef);
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    ExecutorWrapperFactory ef = new ExecutorWrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(ef);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    ExecutorWrapperFactory ef = new ExecutorWrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitCallableTest(ef);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    ExecutorWrapperFactory ef = new ExecutorWrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(ef);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    ExecutorWrapperFactory ef = new ExecutorWrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitCallableFail(ef);
  }

  private class ExecutorWrapperFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private ExecutorWrapperFactory() {
      Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          // ignored
        }
      });
      
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SubmitterExecutorInterface make(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new PriorityScheduledExecutor(poolSize, poolSize, 
                                                                         1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      return new ExecutorWrapper(executor);
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
        it.remove();
      }
    }
  }
  
  private static class TestExecutor implements Executor {
    private Runnable lastCommand = null;
    
    @Override
    public void execute(Runnable command) {
      lastCommand = command;
    }
  }
}

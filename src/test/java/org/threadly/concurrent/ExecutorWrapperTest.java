package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecutorWrapperTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorWrapperFactory();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ExecutorWrapper(null);
    fail("Exception should have thrown");
  }
  
  @Override
  @Test
  public void executeTest() {
    TestExecutor te = new TestExecutor();
    ExecutorWrapper ew = new ExecutorWrapper(te);
    Runnable r = new TestRunnable();
    
    ew.execute(r);
    
    assertTrue(te.lastCommand == r);
    
    super.executeTest();
  }

  private class ExecutorWrapperFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private ExecutorWrapperFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
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
        it.next().shutdownNow();
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

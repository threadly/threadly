package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.Executor;

import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings({"javadoc", "deprecation"})
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
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return new ExecutorWrapper(schedulerFactory.makeSubmitterExecutor(poolSize, prestartIfAvailable));
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
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

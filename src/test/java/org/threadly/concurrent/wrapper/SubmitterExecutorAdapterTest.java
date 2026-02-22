package org.threadly.concurrent.wrapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Executor;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SubmitterExecutorAdapterTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorWrapperFactory();
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new SubmitterExecutorAdapter(null);
      });
  }
  
  @Test
  @Override
  public void executeTest() {
    TestExecutor te = new TestExecutor();
    SubmitterExecutorAdapter ew = new SubmitterExecutorAdapter(te);
    Runnable r = new TestRunnable();
    
    ew.execute(r);
    
    assertTrue(te.lastCommand == r);
    
    super.executeTest();
  }

  private class ExecutorWrapperFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutorAdapter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return new SubmitterExecutorAdapter(schedulerFactory.makeSubmitterExecutor(poolSize, prestartIfAvailable));
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

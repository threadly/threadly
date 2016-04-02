package org.threadly.concurrent.wrapper.intercepter;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.UnfairExecutor;

@SuppressWarnings("javadoc")
public class ExecutorTaskIntercepterInterfaceTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorTaskIntercepterFactory();
  }


  private static class ExecutorTaskIntercepterFactory implements SubmitterExecutorFactory {
    private List<UnfairExecutor> executors = new ArrayList<UnfairExecutor>(1);
    
    @Override
    public ExecutorTaskIntercepter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      UnfairExecutor result = new UnfairExecutor(poolSize);
      executors.add(result);
      
      return new ExecutorTaskIntercepter(result) {
        @Override
        public Runnable wrapTask(Runnable task) {
          return task;
        }
      };
    }

    @Override
    public void shutdown() {
      for (UnfairExecutor ue : executors) {
        ue.shutdownNow();
      }
    }
  }
}

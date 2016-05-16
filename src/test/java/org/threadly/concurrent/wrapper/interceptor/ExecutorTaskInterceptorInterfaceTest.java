package org.threadly.concurrent.wrapper.interceptor;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.UnfairExecutor;

@SuppressWarnings("javadoc")
public class ExecutorTaskInterceptorInterfaceTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorTaskInterceptorFactory();
  }


  private static class ExecutorTaskInterceptorFactory implements SubmitterExecutorFactory {
    private List<UnfairExecutor> executors = new ArrayList<UnfairExecutor>(1);
    
    @Override
    public ExecutorTaskInterceptor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      UnfairExecutor result = new UnfairExecutor(poolSize);
      executors.add(result);
      
      return new ExecutorTaskInterceptor(result) {
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

package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.future.ListenableFuture;

import java.util.concurrent.Callable;

import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;

@SuppressWarnings("javadoc")
public class KeyedExecutorLimiterInterfaceDirectTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyedExecutorLimiterFactory();
  }
  
  private static class KeyedExecutorLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize * 2, prestartIfAvailable);
      
      KeyedExecutorLimiter limiter = new KeyedExecutorLimiter(executor, poolSize);
      return new SubmitterExecutor() {
        @Override
        public void execute(Runnable task) {
          limiter.execute("foo", task);
        }

        @Override
        public <T> ListenableFuture<T> submit(Runnable task, T result) {
          return limiter.submit("foo", task, result);
        }

        @Override
        public <T> ListenableFuture<T> submit(Callable<T> task) {
          return limiter.submit("foo", task);
        }
      };
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}

package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class ConcurrentSimpleSchedulerWrapperTest {
  @Test
  public void executionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.executionTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.executeTestFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.scheduleExecutionTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.scheduleExecutionFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void recurringExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void isTerminatedTest() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.isTerminatedTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.awaitTerminationTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, 
                                          ExecutionException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.submitCallableTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void submitWithResultTest() throws InterruptedException, 
                                            ExecutionException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.submitWithResultTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void futureGetTimeoutFail() throws InterruptedException, 
                                            ExecutionException, 
                                            TimeoutException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.futureGetTimeoutFail(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test (expected = ExecutionException.class)
  public void futureGetExecutionFail() throws InterruptedException, 
                                              ExecutionException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.futureGetExecutionFail(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void futureCancelTest() throws InterruptedException, 
                                        ExecutionException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.futureCancelTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleRunnableTest() throws InterruptedException, 
                                            ExecutionException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.scheduleRunnableTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, 
                                            ExecutionException {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.scheduleCallableTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.scheduleCallableCancelTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.scheduleWithFixedDelayTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayFail() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    ScheduledExecutorService wrapper = new ConcurrentSimpleSchedulerWrapper(executor);
    
    try {
      ScheduledExecutorServiceTest.scheduleWithFixedDelayFail(wrapper);
    } finally {
      executor.shutdown();
    }
  }

  private class SchedulerFactory implements PrioritySchedulerFactory {
    private final List<ScheduledThreadPoolExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<ScheduledThreadPoolExecutor>();
    }
    
    @Override
    public SimpleSchedulerInterface make(int poolSize) {
      ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(poolSize);
      executors.add(executor);
      return new ConcurrentSimpleSchedulerWrapper(executor);
    }
    
    private void shutdown() {
      Iterator<ScheduledThreadPoolExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
      }
    }
  }
}

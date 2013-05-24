package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorServiceWrapperTest {
  private static final int THREAD_COUNT = 1000;
  private static final int KEEP_ALIVE_TIME = 200;
  
  @Test
  public void isTerminatedTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.isTerminatedTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.awaitTerminationTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, 
                                          ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.submitCallableTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void submitWithResultTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.submitWithResultTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void futureGetTimeoutFail() throws InterruptedException, 
                                            ExecutionException, 
                                            TimeoutException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.futureGetTimeoutFail(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test (expected = ExecutionException.class)
  public void futureGetExecutionFail() throws InterruptedException, 
                                              ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.futureGetExecutionFail(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void futureCancelTest() throws InterruptedException, 
                                        ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.futureCancelTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleRunnableTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleRunnableTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, 
                                            ExecutionException {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleCallableTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleCallableCancelTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleWithFixedDelayTest(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayFail() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                                       KEEP_ALIVE_TIME);
    try {
      ScheduledExecutorService wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      ScheduledExecutorServiceTest.scheduleWithFixedDelayFail(wrapper);
    } finally {
      executor.shutdown();
    }
  }
  
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

  private class SchedulerFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SimpleSchedulerInterface make(int poolSize) {
      PriorityScheduledExecutor executor = new PriorityScheduledExecutor(poolSize, poolSize, 200);
      executors.add(executor);
      return new PriorityScheduledExecutorServiceWrapper(executor);
    }
    
    private void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
      }
    }
  }
}

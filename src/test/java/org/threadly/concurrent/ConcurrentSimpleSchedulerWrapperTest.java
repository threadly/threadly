package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.Test;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class ConcurrentSimpleSchedulerWrapperTest {
  @Test
  public void executeTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitRunnableTest(sf);
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitRunnableWithResultTest(sf);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitCallableTest(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.executeFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.scheduleExecutionFail(sf);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledCallableFail(sf);
  }
  
  @Test
  public void recurringExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
  }

  private class SchedulerFactory implements PrioritySchedulerFactory {
    private final List<ScheduledThreadPoolExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<ScheduledThreadPoolExecutor>();
    }
    
    @Override
    public SimpleSchedulerInterface make(int poolSize, boolean prestartIfAvailable) {
      ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(poolSize);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      return new ConcurrentSimpleSchedulerWrapper(executor);
    }
    
    @Override
    public void shutdown() {
      Iterator<ScheduledThreadPoolExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
        it.remove();
      }
    }
  }
}

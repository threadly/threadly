package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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

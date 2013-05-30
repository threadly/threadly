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
  public void executeTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.executeTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitTest(sf);
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
  
  @Test (expected = IllegalArgumentException.class)
  public void submitTestFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitTestFail(sf);
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
  public void submitScheduledExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledExecutionTest(sf);
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
  public void submitScheduledExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledExecutionFail(sf);
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

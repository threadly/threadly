package org.threadly.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorServiceWrapperTest {
  PriorityScheduledExecutorServiceWrapper wrapper;
  
  @Before
  public void setup() {
    wrapper = new PriorityScheduledExecutorServiceWrapper(new PriorityScheduledExecutor(1000, 1000, 200));
  }
  
  @After
  public void tearDown() {
    wrapper.shutdown();
    wrapper = null;
  }
  
  @Test
  public void isTerminatedTest() {
    ScheduledExecutorServiceTest.isTerminatedTest(wrapper);
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    ScheduledExecutorServiceTest.awaitTerminationTest(wrapper);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, 
                                          ExecutionException {
    ScheduledExecutorServiceTest.submitCallableTest(wrapper);
  }
  
  @Test (expected = TimeoutException.class)
  public void futureGetTimeoutFail() throws InterruptedException, 
                                            ExecutionException, 
                                            TimeoutException {
    ScheduledExecutorServiceTest.futureGetTimeoutFail(wrapper);
  }
  
  @Test (expected = ExecutionException.class)
  public void futureGetExecutionFail() throws InterruptedException, 
                                              ExecutionException {
    ScheduledExecutorServiceTest.futureGetExecutionFail(wrapper);
  }
  
  @Test
  public void futureCancelTest() throws InterruptedException, 
                                        ExecutionException {
    ScheduledExecutorServiceTest.futureCancelTest(wrapper);
  }
  
  @Test
  public void scheduleRunnableTest() throws InterruptedException, 
                                            ExecutionException {
    ScheduledExecutorServiceTest.scheduleRunnableTest(wrapper);
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, 
                                            ExecutionException {
    ScheduledExecutorServiceTest.scheduleCallableTest(wrapper);
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    ScheduledExecutorServiceTest.scheduleCallableCancelTest(wrapper);
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    ScheduledExecutorServiceTest.scheduleWithFixedDelayTest(wrapper);
  }
  
  @Test
  public void scheduleWithFixedDelayFail() {
    ScheduledExecutorServiceTest.scheduleWithFixedDelayFail(wrapper);
  }
}

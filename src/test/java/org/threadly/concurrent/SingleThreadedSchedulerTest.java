package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.SchedulerServiceInterfaceTest.SchedulerServiceFactory;

@SuppressWarnings("javadoc")
public class SingleThreadedSchedulerTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @Test
  public void isShutdownTest() {
    SingleThreadedScheduler sts = new SingleThreadedScheduler();
    assertFalse(sts.isShutdown());
    
    sts.shutdown();
    
    assertTrue(sts.isShutdown());
  }
  
  @Test
  public void executeTest() {
    SubmitterExecutorInterfaceTest.executeTest(new SingleThreadedSchedulerFactory());
  }
  
  @Test
  public void executeWithFailureRunnableTest() {
    SubmitterExecutorInterfaceTest.executeWithFailureRunnableTest(new SingleThreadedSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    SubmitterExecutorInterfaceTest.executeFail(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitRunnableTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitRunnableExceptionTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitRunnableExceptionTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitRunnableWithResultExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultExceptionTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitCallableTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitCallableExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitCallableExceptionTest(new SingleThreadedSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SubmitterExecutorInterfaceTest.submitRunnableFail(new SingleThreadedSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableWithResultFail() {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultFail(new SingleThreadedSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SubmitterExecutorInterfaceTest.submitCallableFail(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void scheduleTest() {
    SimpleSchedulerInterfaceTest.scheduleTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void scheduleNoDelayTest() {
    SimpleSchedulerInterfaceTest.scheduleNoDelayTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void scheduleFail() {
    SimpleSchedulerInterfaceTest.scheduleFail(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void recurringExecutionTest() {
    SimpleSchedulerInterfaceTest.recurringExecutionTest(false, new SingleThreadedSchedulerFactory());
  }

  @Test
  public void recurringExecutionWithInitialDelayTest() {
    SimpleSchedulerInterfaceTest.recurringExecutionTest(true, new SingleThreadedSchedulerFactory());
  }

  @Test
  public void recurringExecutionFail() {
    SimpleSchedulerInterfaceTest.recurringExecutionFail(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitScheduledRunnableTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitScheduledRunnableFail() {
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void submitScheduledCallableFail() {
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void removeRunnableTest() {
    SchedulerServiceInterfaceTest.removeRunnableTest(new SingleThreadedSchedulerFactory());
  }

  @Test
  public void removeCallableTest() {
    SchedulerServiceInterfaceTest.removeCallableTest(new SingleThreadedSchedulerFactory());
  }

  private class SingleThreadedSchedulerFactory implements SchedulerServiceFactory {
    private final List<SingleThreadedScheduler> schedulers = new LinkedList<SingleThreadedScheduler>();

    @Override
    public void shutdown() {
      Iterator<SingleThreadedScheduler> it = schedulers.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
        it.remove();
      }
    }

    @Override
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      SingleThreadedScheduler sts = new SingleThreadedScheduler();
      schedulers.add(sts);
      
      return sts;
    }
  }
}

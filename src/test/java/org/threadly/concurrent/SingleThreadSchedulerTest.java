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
public class SingleThreadSchedulerTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @Test
  public void isShutdownTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    assertFalse(sts.isShutdown());
    
    sts.shutdown();
    
    assertTrue(sts.isShutdown());
  }
  
  @Test
  public void executeTest() {
    SubmitterExecutorInterfaceTest.executeTest(new SingleThreadSchedulerFactory());
  }
  
  @Test
  public void executeWithFailureRunnableTest() {
    SubmitterExecutorInterfaceTest.executeWithFailureRunnableTest(new SingleThreadSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    SubmitterExecutorInterfaceTest.executeFail(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitRunnableTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitRunnableExceptionTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitRunnableExceptionTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitRunnableWithResultExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultExceptionTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitCallableTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitCallableExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitCallableExceptionTest(new SingleThreadSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SubmitterExecutorInterfaceTest.submitRunnableFail(new SingleThreadSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableWithResultFail() {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultFail(new SingleThreadSchedulerFactory());
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SubmitterExecutorInterfaceTest.submitCallableFail(new SingleThreadSchedulerFactory());
  }

  @Test
  public void scheduleTest() {
    SimpleSchedulerInterfaceTest.scheduleTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void scheduleNoDelayTest() {
    SimpleSchedulerInterfaceTest.scheduleNoDelayTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void scheduleFail() {
    SimpleSchedulerInterfaceTest.scheduleFail(new SingleThreadSchedulerFactory());
  }

  @Test
  public void recurringExecutionTest() {
    SimpleSchedulerInterfaceTest.recurringExecutionTest(false, new SingleThreadSchedulerFactory());
  }

  @Test
  public void recurringExecutionWithInitialDelayTest() {
    SimpleSchedulerInterfaceTest.recurringExecutionTest(true, new SingleThreadSchedulerFactory());
  }

  @Test
  public void recurringExecutionFail() {
    SimpleSchedulerInterfaceTest.recurringExecutionFail(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitScheduledRunnableTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitScheduledRunnableFail() {
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(new SingleThreadSchedulerFactory());
  }

  @Test
  public void submitScheduledCallableFail() {
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(new SingleThreadSchedulerFactory());
  }

  @Test
  public void removeRunnableTest() {
    SchedulerServiceInterfaceTest.removeRunnableTest(new SingleThreadSchedulerFactory());
  }

  @Test
  public void removeCallableTest() {
    SchedulerServiceInterfaceTest.removeCallableTest(new SingleThreadSchedulerFactory());
  }

  private class SingleThreadSchedulerFactory implements SchedulerServiceFactory {
    private final List<SingleThreadScheduler> schedulers = new LinkedList<SingleThreadScheduler>();

    @Override
    public void shutdown() {
      Iterator<SingleThreadScheduler> it = schedulers.iterator();
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
      SingleThreadScheduler sts = new SingleThreadScheduler();
      schedulers.add(sts);
      
      return sts;
    }
  }
}

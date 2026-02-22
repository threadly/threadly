package org.threadly.concurrent.wrapper.interceptor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SchedulerService;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class SchedulerServiceTaskInterceptorTest extends SubmitterSchedulerTaskInterceptorTest {
  @BeforeEach
  @Override
  public void setup() {
    scheduler = new TestableScheduler();
    submitterSchedulerInterceptor = new TestSchedulerServiceInterceptor(scheduler);
    executorInterceptor = submitterSchedulerInterceptor;
    testInterceptor = (TestInterceptor)executorInterceptor;
    interceptedTasks = new ArrayList<>(1);
    submitSchedulerTaskInterceptorLamba = new SchedulerServiceTaskInterceptor(scheduler, (r1, b1) -> { 
      interceptedTasks.add(r1);
      
      return DoNothingRunnable.instance();
    });  
    tr = new TestRunnable();
  }
  
  @Override
  @Test
  public void defaultConstructorFail() {
      assertThrows(RuntimeException.class, () -> {
      SchedulerServiceTaskInterceptor ssti = new SchedulerServiceTaskInterceptor(scheduler);
      ssti.schedule(DoNothingRunnable.instance(), 10);
      });
  }

  @Override
  @Test
  public void nullConstructorFail() {
      assertThrows(RuntimeException.class, () -> {
      @SuppressWarnings("unused")
      SchedulerServiceTaskInterceptor ssti = new SchedulerServiceTaskInterceptor(scheduler, null);
      });
  }

  private static class TestSchedulerServiceInterceptor extends SchedulerServiceTaskInterceptor 
                                                       implements TestInterceptor {
    private final List<Runnable> interceptedTasks;
    
    public TestSchedulerServiceInterceptor(SchedulerService parentScheduler) {
      super(parentScheduler);
      
      interceptedTasks = new ArrayList<>(1);
    }

    @Override
    public List<Runnable> getInterceptedTasks() {
      return interceptedTasks;
    }

    @Override
    public Runnable wrapTask(Runnable task, boolean recurring) {
      interceptedTasks.add(task);
      
      return DoNothingRunnable.instance();
    }
  }
}

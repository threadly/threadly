package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.AbstractPrioritySchedulerTest;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerSubPoolTest extends AbstractPrioritySchedulerTest {
  @Override
  protected AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory() {
    return new SingleThreadSubPoolFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;
  }
  
  @Test
  public void executeLimitTest() throws InterruptedException, TimeoutException {
    PriorityScheduler ps = new PriorityScheduler(16);
    ps.prestartAllThreads();
    try {
      Executor limitedExecutor = new SingleThreadSchedulerSubPool(ps);
      final AtomicInteger running = new AtomicInteger(0);
      final AsyncVerifier verifier = new AsyncVerifier();
      List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable(20) {
          @Override
          public void handleRunStart() {
            int runningCount = running.incrementAndGet();
            if (runningCount > 1) {
              verifier.fail(runningCount + " currently running");
            }
          }
          
          @Override
          public void handleRunFinish() {
            running.decrementAndGet();
            verifier.signalComplete();
          }
        };
        limitedExecutor.execute(tr);
        runnables.add(tr);
      }
      
      verifier.waitForTest(1000 * 10, TEST_QTY);
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
    } finally {
      ps.shutdownNow();
    }
  }

  protected static class SingleThreadSubPoolFactory implements AbstractPrioritySchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
    
    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize, 
                                                                   TaskPriority defaultPriority, 
                                                                   long maxWaitForLowPriority) {
      return new SingleThreadSchedulerSubPool(schedulerFactory.makeAbstractPriorityScheduler(poolSize), 
                                              defaultPriority, maxWaitForLowPriority);
    }
    
    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize) {
      return new SingleThreadSchedulerSubPool(schedulerFactory.makeAbstractPriorityScheduler(poolSize));
    }
  }
}

package org.threadly.concurrent.processing;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.AsyncVerifier;

@SuppressWarnings("javadoc")
public class FlowControlledNoFailureProcessorTest {
  private PriorityScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new PriorityScheduler(Runtime.getRuntime().availableProcessors());
    scheduler.prestartAllThreads();
  }
  
  @After
  public void cleanup() {
    scheduler.shutdownNow();
  }
  
  @Test
  public void concurrentLimitTest() throws ExecutionException, InterruptedException, TimeoutException {
    concurrentLimitTest(false);
  }
  
  @Test
  public void concurrentLimitInOrderTest() throws ExecutionException, InterruptedException, TimeoutException {
    concurrentLimitTest(true);
  }
  
  public void concurrentLimitTest(boolean inOrder) throws ExecutionException, InterruptedException, TimeoutException {
    final int maxRunning = 2;
    final int totalRunCount = 100;
    AtomicInteger running = new AtomicInteger(0);
    AsyncVerifier verifier = new AsyncVerifier();
    
    FlowControlledProcessor<Void> processor = new FlowControlledNoFailureProcessor<Void>(maxRunning, inOrder) {
      private int count = 0;
      
      @Override
      protected boolean hasNext() {
        return count < totalRunCount;
      }

      @Override
      protected ListenableFuture<Void> next() {
        count++;
        verifier.assertTrue(running.incrementAndGet() <= maxRunning);
        ListenableFuture<Void> result = 
            scheduler.submitScheduled(DoNothingRunnable.instance(), null, 
                                      ThreadLocalRandom.current().nextInt(5) + 5);
        result.listener(running::decrementAndGet);
        return result;
      }

      @Override
      protected void handleResult(Void result) {
        verifier.signalComplete();
      }
    };
    ListenableFuture<?> runFuture = processor.start();
    
    verifier.waitForTest(10_000, totalRunCount);
    runFuture.get(1_000, TimeUnit.MILLISECONDS);
    assertTrue(runFuture.isDone());
  }
  
  @Test
  public void inOrderCompletionTest() throws InterruptedException, TimeoutException {
    final int maxRunning = Runtime.getRuntime().availableProcessors();
    final int totalRunCount = 100 * maxRunning;
    AsyncVerifier verifier = new AsyncVerifier();
    
    FlowControlledProcessor<Integer> processor = new FlowControlledNoFailureProcessor<Integer>(maxRunning, true) {
      private int count = 0;
      private int last = 0;
      
      @Override
      protected boolean hasNext() {
        return count < totalRunCount;
      }

      @Override
      protected ListenableFuture<Integer> next() {
        return scheduler.submitScheduled(DoNothingRunnable.instance(), ++count, 
                                         ThreadLocalRandom.current().nextInt(20));
      }

      @Override
      protected void handleResult(Integer result) {
        verifier.assertEquals(last + 1, result);
        last = result;
        verifier.signalComplete();
      }

      @Override
      protected boolean handleFailure(Throwable t) {
        if (! (t instanceof AsyncVerifier.TestFailure)) {
          verifier.fail(t);
        }
        return false;
      }
    };
    processor.start();
    
    verifier.waitForTest(10_000, totalRunCount);
  }
  
  @Test
  public void unhandledErrorTest() throws InterruptedException, TimeoutException {
    final RuntimeException expectedException = new RuntimeException();
    final int totalRunCount = 100;
    
    FlowControlledProcessor<Void> processor = new FlowControlledNoFailureProcessor<Void>(2, false) {
      private int count = 0;
      
      @Override
      protected boolean hasNext() {
        return count < totalRunCount;
      }

      @Override
      protected ListenableFuture<Void> next() {
        Runnable task;
        if (count++ == 1) {
          task = () -> { throw expectedException; };
        } else {
          task = DoNothingRunnable.instance();
        }
        return scheduler.submitScheduled(task, null, ThreadLocalRandom.current().nextInt(5) + 5);
      }

      @Override
      protected void handleResult(Void result) {
        // ignored
      }
    };
    ListenableFuture<?> runFuture = processor.start();
    
    try {
      runFuture.get(10_000, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == expectedException);
    }
  }
}

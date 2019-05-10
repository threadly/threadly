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
public class FlowControlledNoResultProcessorTest {
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
    final RuntimeException expectedException = new RuntimeException();
    final int maxRunning = 2;
    final int totalRunCount = 100;
    AtomicInteger running = new AtomicInteger(0);
    AsyncVerifier verifier = new AsyncVerifier();
    
    FlowControlledProcessor<?> processor = new FlowControlledNoResultProcessor(maxRunning) {
      private int count = 0;
      
      @Override
      protected boolean hasNext() {
        return count < totalRunCount;
      }

      @Override
      protected ListenableFuture<Void> next() {
        verifier.assertTrue(running.incrementAndGet() <= maxRunning);
        Runnable task;
        if (count++ % 2 == 0) {
          task = DoNothingRunnable.instance();
        } else {
          task = () -> { throw expectedException; };
        }
        ListenableFuture<Void> result = 
            scheduler.submitScheduled(task, null, ThreadLocalRandom.current().nextInt(5) + 5);
        result.listener(running::decrementAndGet);
        result.listener(verifier::signalComplete);
        return result;
      }

      @Override
      protected boolean handleFailure(Throwable t) {
        if (t == expectedException) {
          verifier.signalComplete();
          return true;
        } else if (! (t instanceof AsyncVerifier.TestFailure)) {
          verifier.fail(t);
        }
        return false;
      }
    };
    ListenableFuture<?> runFuture = processor.start();
    
    verifier.waitForTest(10_000, totalRunCount);
    runFuture.get(1_000, TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void handledErrorTest() throws ExecutionException, InterruptedException, TimeoutException {
    final RuntimeException expectedException = new RuntimeException();
    final int maxRunning = 2;
    final int totalRunCount = 100;
    AsyncVerifier verifier = new AsyncVerifier();
    
    FlowControlledProcessor<?> processor = new FlowControlledNoResultProcessor(maxRunning) {
      private int count = 0;
      
      @Override
      protected boolean hasNext() {
        return count < totalRunCount;
      }

      @Override
      protected ListenableFuture<?> next() {
        if (ThreadLocalRandom.current().nextBoolean()) {
          count++;
          throw expectedException;
        }
        Runnable task;
        if (count++ % 2 == 0) {
          task = DoNothingRunnable.instance();
        } else {
          task = () -> { throw expectedException; };
        }
        return scheduler.submitScheduled(task, null, ThreadLocalRandom.current().nextInt(5) + 5)
                        .listener(verifier::signalComplete);
      }

      @Override
      protected boolean handleFailure(Throwable t) {
        if (t == expectedException) {
          verifier.signalComplete();
          return true;
        } else if (! (t instanceof AsyncVerifier.TestFailure)) {
          verifier.fail(t);
        }
        return false;
      }
    };
    ListenableFuture<?> runFuture = processor.start();
    
    verifier.waitForTest(10_000, totalRunCount);
    runFuture.get(1_000, TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void unhandledErrorTest() throws InterruptedException, TimeoutException {
    final RuntimeException expectedException = new RuntimeException();
    final int totalRunCount = 100;
    
    FlowControlledProcessor<?> processor = new FlowControlledNoResultProcessor(2) {
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

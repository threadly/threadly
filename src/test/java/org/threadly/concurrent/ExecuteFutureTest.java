package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestablePriorityScheduler;

@SuppressWarnings("javadoc")
public class ExecuteFutureTest {
  private static final TestablePriorityScheduler scheduler = new TestablePriorityScheduler(new PriorityScheduledExecutor(10, 10, 500));
  
  public static void blockTillCompletedTest(FutureFactory ff) {
    Runnable r = new TestRunnable();
    ExecuteFuture future = ff.make(r, scheduler.makeLock());
    BlockingRunnable br = new BlockingRunnable(future);
    
    scheduler.execute(br);
    
    scheduler.tick();
    assertTrue(br.runStarted);
    assertFalse(br.blockReleased);
    assertFalse(br.executionExceptionThrown);
    
    scheduler.execute((Runnable)future);
    
    scheduler.tick();
    assertTrue(br.runStarted);
    assertTrue(br.blockReleased);
    assertFalse(br.executionExceptionThrown);
  }
  
  public static void blockTillCompletedFail(FutureFactory ff) {
    Runnable r = new FailureRunnable();
    ExecuteFuture future = ff.make(r, scheduler.makeLock());
    BlockingRunnable br = new BlockingRunnable(future);
    
    scheduler.execute(br);
    
    scheduler.tick();
    assertTrue(br.runStarted);
    assertFalse(br.blockReleased);
    assertFalse(br.executionExceptionThrown);
    
    scheduler.execute((Runnable)future);
    
    scheduler.tick();
    assertTrue(br.runStarted);
    assertTrue(br.blockReleased);
    assertTrue(br.executionExceptionThrown);
  }
  
  public static void isCompletedTest(FutureFactory ff) {
    TestRunnable r = new TestRunnable();
    ExecuteFuture future = ff.make(r, new NativeLock());
    scheduler.getExecutor().execute((Runnable)future);
    try {
      future.blockTillCompleted();
    } catch (InterruptedException e) {
      // ignored
    } catch (ExecutionException e) {
      // ignored
    }
    
    assertTrue(future.isCompleted());
  }
  
  public static void isCompletedFail(FutureFactory ff) {
    TestRunnable r = new FailureRunnable();
    ExecuteFuture future = ff.make(r, new NativeLock());
    scheduler.getExecutor().execute((Runnable)future);
    try {
      future.blockTillCompleted();
    } catch (InterruptedException e) {
      // ignored
    } catch (ExecutionException e) {
      // ignored
    }
    
    assertTrue(future.isCompleted());
  }
  
  public interface FutureFactory {
    public ExecuteFuture make(Runnable run, 
                              VirtualLock lock);
  }
  
  private static class BlockingRunnable extends VirtualRunnable {
    private final ExecuteFuture future;
    private volatile boolean runStarted;
    private volatile boolean blockReleased;
    private volatile boolean executionExceptionThrown;
    
    public BlockingRunnable(ExecuteFuture future) {
      this.future = future;
      runStarted = false;
      blockReleased = false;
      executionExceptionThrown = false;
    }

    @Override
    public void run() {
      runStarted = true;
      try {
        future.blockTillCompleted();
      } catch (InterruptedException e) {
        // ignored
      } catch (ExecutionException e) {
        executionExceptionThrown = true;
        // ignored
      } finally {
        blockReleased = true;
      }
    }
  }
  
  private static class FailureRunnable extends TestRunnable {
    @Override
    public void handleRunFinish() {
      throw new RuntimeException();
    }
  }
}

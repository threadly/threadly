package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class TaskExecutorDistributorTest {
  private static final int PARALLEL_LEVEL = 100;
  private static final int RUNNABLE_COUNT_PER_LEVEL = 1000;
  
  private volatile boolean ready;
  private PriorityScheduledExecutor scheduler;
  private TaskExecutorDistributor distributor;
  
  @Before
  public void setup() {
    scheduler = new PriorityScheduledExecutor(PARALLEL_LEVEL + 1, 
                                              PARALLEL_LEVEL * 2, 
                                              1000 * 10, 
                                              TaskPriority.High, 
                                              PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    StripedLock sLock = new StripedLock(PARALLEL_LEVEL, new NativeLockFactory()); // TODO - test with testable lock
    distributor = new TaskExecutorDistributor(scheduler, sLock);
    ready = false;
  }
  
  @After
  public void tearDown() {
    scheduler.shutdown();
    scheduler = null;
    distributor = null;
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new TaskExecutorDistributor(1, null);
    
    fail("Exception should have been thrown");
  }
  
  @Test
  public void testGetExecutor() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  public void testExecutes() {
    final Object testLock = new Object();
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);

    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        synchronized (testLock) {
          for (int i = 0; i < PARALLEL_LEVEL; i++) {
            ThreadContainer tc = new ThreadContainer();
            TDRunnable previous = null;
            for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
              TDRunnable tr = new TDRunnable(tc, previous);
              runs.add(tr);
              distributor.addTask(tc, tr);
              
              previous = tr;
            }
          }
            
          ready = true;
        }
      }
    });
    
    // block till ready to ensure other thread got lock
    new TestCondition() {
      @Override
      public boolean get() {
        return ready;
      }
    }.blockTillTrue(20000);

    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillFinished(10 * 1000);
        assertEquals(tr.getRunCount(), 1); // verify each only ran once
        assertTrue(tr.threadTracker.threadConsistent);  // verify that all threads for a given key ran in the same thread
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
      }
    }
  }
  
  @Test
  public void testExecuteFail() {
    try {
      distributor.addTask(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      distributor.addTask(new Object(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  private class TDRunnable extends TestRunnable {
    private final TDRunnable previousRunnable;
    private final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    
    private TDRunnable(ThreadContainer threadTracker, 
                       TDRunnable previousRunnable) {
      this.threadTracker = threadTracker;
      this.previousRunnable = previousRunnable;
      previousRanFirst = false;
    }
    
    @Override
    public void handleRunStart() {
      threadTracker.running();
      
      if (previousRunnable != null) {
        previousRanFirst = previousRunnable.ranOnce();
      } else {
        previousRanFirst = true;
      }
    }
  }
  
  private class ThreadContainer {
    private Thread runningThread = null;
    private boolean threadConsistent = true;
    
    public synchronized void running() {
      while (! ready) {
        // spin
      }
      
      if (runningThread == null) {
        runningThread = Thread.currentThread();
      } else {
        threadConsistent = threadConsistent && runningThread.equals(Thread.currentThread());
      }
    }
  }
}

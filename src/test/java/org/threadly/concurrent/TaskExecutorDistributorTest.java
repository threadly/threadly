package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.StripedLock;
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
  
  @Test
  public void constructorFail() {
    try {
      new TaskExecutorDistributor(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new TaskExecutorDistributor(scheduler, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getExecutorTest() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  public void keyBasedExecuteConsistentThreadTest() {
    final Object testLock = new Object();
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);

    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        synchronized (testLock) {
          for (int i = 0; i < PARALLEL_LEVEL; i++) {
            ThreadContainer tc = new ThreadContainer();
            Executor keyExecutor = distributor.getExecutorForKey(tc);
            TDRunnable previous = null;
            for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
              TDRunnable tr = new TDRunnable(tc, previous);
              runs.add(tr);
              keyExecutor.execute(tr);
              
              previous = tr;
            }
          }
            
          ready = true;
        }
      }
    });
    
    // block till ready to ensure other thread got testLock lock
    new TestCondition() {
      @Override
      public boolean get() {
        return ready;
      }
    }.blockTillTrue(20 * 1000, 100);

    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillFinished(20 * 1000);
        assertEquals(tr.getRunCount(), 1); // verify each only ran once
        assertTrue(tr.threadTracker.threadConsistent);  // verify that all threads for a given key ran in the same thread
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
      }
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getExecutorForKeyFail() {
    distributor.getExecutorForKey(null);
  }
  
  @Test
  public void executeConsistentThreadTest() {
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
    
    // block till ready to ensure other thread got testLock lock
    new TestCondition() {
      @Override
      public boolean get() {
        return ready;
      }
    }.blockTillTrue(20 * 1000, 100);

    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillFinished(20 * 1000);
        assertEquals(tr.getRunCount(), 1); // verify each only ran once
        assertTrue(tr.threadTracker.threadConsistent);  // verify that all threads for a given key ran in the same thread
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
      }
    }
  }
  
  @Test
  public void submitRunnableFail() {
    try {
      distributor.submitTask(null, new TestRunnable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), null, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableConsistentThreadTest() {
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
              distributor.submitTask(tc, tr);
              
              previous = tr;
            }
          }
            
          ready = true;
        }
      }
    });
    
    // block till ready to ensure other thread got testLock lock
    new TestCondition() {
      @Override
      public boolean get() {
        return ready;
      }
    }.blockTillTrue(20 * 1000, 100);

    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillFinished(20 * 1000);
        assertEquals(tr.getRunCount(), 1); // verify each only ran once
        assertTrue(tr.threadTracker.threadConsistent);  // verify that all threads for a given key ran in the same thread
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
      }
    }
  }
  
  @Test
  public void submitCallableFail() {
    try {
      distributor.submitTask(null, VirtualCallable.fromRunnable(new TestRunnable(), null));
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  /*@Test
  public void submitCallableConsistentThreadTest() {
    // TODO -- implement
  }*/

  @Test
  public void executeStressTest() {
    final Object testLock = new Object();
    final int expectedCount = (PARALLEL_LEVEL * 2) * (RUNNABLE_COUNT_PER_LEVEL * 2);
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(expectedCount);

    ready = true; // don't make tasks wait
    
    scheduler.execute(new Runnable() {
      private final Map<Integer, ThreadContainer> containers = new HashMap<Integer, ThreadContainer>();
      private final Map<Integer, TDRunnable> previousRunnables = new HashMap<Integer, TDRunnable>();
      
      @Override
      public void run() {
        synchronized (testLock) {
          for (int i = 0; i < RUNNABLE_COUNT_PER_LEVEL * 2; i++) {
            for (int j = 0; j < PARALLEL_LEVEL * 2; j++) {
              ThreadContainer tc = containers.get(j);
              if (tc == null) {
                tc = new ThreadContainer();
                containers.put(j, tc);
              }
              
              TDRunnable tr = new TDRunnable(tc, previousRunnables.get(j)) {
                private boolean added = false;
                
                @Override
                public void handleRunFinish() {
                  if (! added) {
                    distributor.addTask(threadTracker, this);
                    added = true;
                  }
                }
              };
              runs.add(tr);
              distributor.addTask(tc, tr);
              previousRunnables.put(j, tr);
            }
          }
        }
      }
    });
    
    // block till ready to ensure other thread got testLock lock
    new TestCondition() {
      @Override
      public boolean get() {
        synchronized (testLock) {
          return runs.size() == expectedCount;
        }
      }
    }.blockTillTrue(20 * 1000, 100);

    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillFinished(20 * 1000, 2);
        assertEquals(tr.getRunCount(), 2); // verify each only ran twice
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
      }
    }
  }
  
  @Test
  public void addTaskFail() {
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
    protected final TDRunnable previousRunnable;
    protected final ThreadContainer threadTracker;
    protected volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    private TDRunnable(ThreadContainer threadTracker, 
                       TDRunnable previousRunnable) {
      this.threadTracker = threadTracker;
      this.previousRunnable = previousRunnable;
      previousRanFirst = false;
      verifiedPrevious = false;
    }
    
    @Override
    public void handleRunStart() {
      threadTracker.running();
      
      if (! verifiedPrevious) {
        if (previousRunnable != null) {
          previousRanFirst = previousRunnable.getRunCount() >= 1;
        } else {
          previousRanFirst = true;
        }
        
        verifiedPrevious = true;
      }
    }
  }
  
  private class ThreadContainer {
    private Thread runningThread = null;
    private boolean threadConsistent = true;
    
    public synchronized void running() {
      while (! ready) {
        LockSupport.parkNanos(1000000 * 10);
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

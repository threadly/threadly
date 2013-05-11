package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class TaskSchedulerDistributorTest {
  private static final int PARALLEL_LEVEL = 2;
  private static final int RUNNABLE_COUNT_PER_LEVEL = 5;
  
  private volatile boolean ready;
  private PriorityScheduledExecutor scheduler;
  private Object agentLock;
  private TaskSchedulerDistributor distributor;
  
  @Before
  public void setup() {
    scheduler = new PriorityScheduledExecutor(PARALLEL_LEVEL + 1, 
                                              PARALLEL_LEVEL * 2, 
                                              1000 * 10, 
                                              TaskPriority.High, 
                                              PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT);
    agentLock = new Object();
    distributor = new TaskSchedulerDistributor(scheduler, agentLock);
    ready = false;
  }
  
  @After
  public void tearDown() {
    scheduler.shutdown();
    scheduler = null;
    agentLock = null;
    distributor = null;
    ready = false;
  }
  
  private List<TDRunnable> populate(final Object testLock, 
                                    final AddHandler ah) {
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        // hold agent lock to prevent execution till ready
        synchronized (agentLock) {
          synchronized (testLock) {
            for (int i = 0; i < PARALLEL_LEVEL; i++) {
              ThreadContainer tc = new ThreadContainer();
              TDRunnable previous = null;
              for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
                TDRunnable tr = new TDRunnable(tc, previous);
                runs.add(tr);
                ah.addTDRunnable(tc, tr);
                
                previous = tr;
              }
            }
            
            ready = true;
          }
        }
      }
    });
    
    // block till ready to ensure other thread got lock
    new TestCondition() {
      @Override
      public boolean get() {
        return ready;
      }
    }.blockTillTrue();
    
    return runs;
  }
  
  @Test
  public void testGetExecutor() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  public void testExecutes() {
    final Object testLock = new Object();
    
    List<TDRunnable> runs = populate(testLock, 
                                     new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.addTask(key, tdr);
      }
    });
    
    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillRun(1000);
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
  
  @Test
  public void scheduleExecutionTest() {
    final int scheduleDelay = 50;

    final Object testLock = new Object();
    
    List<TDRunnable> runs = populate(testLock, 
                                     new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.schedule(key, tdr, scheduleDelay);
      }
    });
    
    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        tr.blockTillRun(1000);
        assertEquals(tr.getRunCount(), 1); // verify each only ran once
        assertTrue(tr.getDelayTillFirstRun() >= scheduleDelay);
        assertTrue(tr.threadTracker.runningConsistent);  // verify that it never run in parallel
      }
    }
  }
  
  @Test
  public void scheduleExecutionFail() {
    try {
      distributor.schedule(new Object(), null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.schedule(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.schedule(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void recurringExecutionTest() {
    final int recurringDelay = 50;

    final Object testLock = new Object();
    
    List<TDRunnable> runs = populate(testLock, 
                                     new AddHandler() {
      int initialDelay = 0;
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.scheduleWithFixedDelay(key, tdr, initialDelay++, 
                                           recurringDelay);
      }
    });
    
    synchronized (testLock) {
      Iterator<TDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        TDRunnable tr = it.next();
        assertTrue(tr.getDelayTillRun(2) >= recurringDelay);
        tr.blockTillRun(10 * 1000, 3);
        assertTrue(tr.threadTracker.runningConsistent);  // verify that it never run in parallel
      }
    }
  }
  
  @Test
  public void recurringExecutionFail() {
    try {
      distributor.scheduleWithFixedDelay(new Object(), null, 1000, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleWithFixedDelay(new Object(), new TestRunnable(), -1, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleWithFixedDelay(new Object(), new TestRunnable(), 100, -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleWithFixedDelay(null, new TestRunnable(), 100, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  private interface AddHandler {
    public void addTDRunnable(Object key, TDRunnable tdr);
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
      threadTracker.done();
    }
  }
  
  private class ThreadContainer {
    private Thread runningThread = null;
    private boolean threadConsistent = true;
    private boolean running = false;
    private boolean runningConsistent = true;
    
    public synchronized void running() {
      if (running) {
        runningConsistent = false;
      }
      running = true;
      if (runningThread != null) {
        threadConsistent = threadConsistent && runningThread.equals(Thread.currentThread());
      }
      runningThread = Thread.currentThread();
    }

    public synchronized void done() {
      if (! running) {
        runningConsistent = false;
      }
      running = false;
    }
    
    @Override
    public String toString() {
      return Integer.toHexString(System.identityHashCode(this));
    }
  }
}

package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.TestUtil;

public class TaskDistributorTest {
  private static final int PARALLEL_LEVEL = 1;
  private static final int RUNNABLE_COUNT_PER_LEVEL = 1000;
  private static final int DELAY_EXECTION_TIME = 1000;
  
  private volatile boolean ready;
  private PriorityScheduledExecutor scheduler;
  private Object agentLock;
  private TaskDistributor distributor;
  
  @Before
  public void setup() {
    scheduler = new PriorityScheduledExecutor(PARALLEL_LEVEL + 1, 
                                              PARALLEL_LEVEL * 2, 
                                              1000 * 10, 
                                              TaskPriority.High, 
                                              PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT);
    agentLock = new Object();
    distributor = new TaskDistributor(scheduler, agentLock);
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
  
  /* commented out because this currently does not work
  @Test
  public void testExecutes() {
    final List<TestRunnable> runs = new ArrayList<TestRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);

    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        synchronized (agentLock) {
          long startTime = System.currentTimeMillis();

          for (int i = 0; i < PARALLEL_LEVEL; i++) {
            Object key = new Object();
            ThreadContainer tc = new ThreadContainer();
            for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
              TestRunnable tr = new TestRunnable(tc);
              runs.add(tr);
              distributor.addTask(key, tr);
            }
          }
          
            while (System.currentTimeMillis() - startTime < DELAY_EXECTION_TIME) {
            // spin
          }
          ready = true;
          System.out.println("ready");
        }
      }
    });
    
    TestUtil.sleep(DELAY_EXECTION_TIME);
    
    while (! ready) {
      // spin
    }
    
    TestUtil.sleep(100);
    
    Iterator<TestRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      assertEquals(tr.ranCount, 1);
      assertTrue(tr.threadTracker.threadConsistent);
    }
  }*/
  
  private class TestRunnable implements Runnable {
    private final ThreadContainer threadTracker;
    private int ranCount = 0;
    
    private TestRunnable(ThreadContainer threadTracker) {
      this.threadTracker = threadTracker;
    }
    
    @Override
    public void run() {
      ranCount++;
      threadTracker.running();
    }
  }
  
  private class ThreadContainer {
    private Thread runningThread = null;
    private boolean threadConsistent = true;
    
    public synchronized void running() {
      System.out.println(this + " - " + Thread.currentThread());
      if (runningThread == null) {
        runningThread = Thread.currentThread();
      } else {
        threadConsistent = threadConsistent && runningThread.equals(Thread.currentThread());
      }
    }
  }
  
}

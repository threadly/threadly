package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest.SubmitterExecutorFactory;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class TaskExecutorDistributorTest {
  private static final int PARALLEL_LEVEL = TEST_QTY;
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY * 2;
  
  private static PriorityScheduledExecutor scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduledExecutor(PARALLEL_LEVEL + 1, 
                                                    PARALLEL_LEVEL * 2, 
                                                    1000 * 10, 
                                                    TaskPriority.High, 
                                                    PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @AfterClass
  public static void tearDownClass() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  private Object agentLock;
  private TaskExecutorDistributor distributor;
  
  @Before
  public void setup() {
    StripedLock sLock = new StripedLock(1);
    agentLock = sLock.getLock(null);  // there should be only one lock
    distributor = new TaskExecutorDistributor(scheduler, sLock, Integer.MAX_VALUE, false);
  }
  
  @After
  public void tearDown() {
    agentLock = null;
    distributor = null;
  }
  
  private List<TDRunnable> populate(AddHandler ah) {
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to prevent execution till ready
    synchronized (agentLock) {
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
    }
    
    return runs;
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorTest() {
    // none should throw exception
    new TaskExecutorDistributor(scheduler);
    new TaskExecutorDistributor(scheduler, 1);
    new TaskExecutorDistributor(1, scheduler);
    new TaskExecutorDistributor(1, scheduler, 1);
    StripedLock sLock = new StripedLock(1);
    new TaskExecutorDistributor(scheduler, sLock, 1, false);
  }
  
  @SuppressWarnings({ "deprecation", "unused" })
  @Test
  public void constructorFail() {
    try {
      new TaskExecutorDistributor(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new TaskExecutorDistributor(scheduler, null, 
                                  Integer.MAX_VALUE, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new TaskExecutorDistributor(1, 1, -1);
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
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        @SuppressWarnings("deprecation")
        Executor keySubmitter = distributor.getExecutorForKey(key);
        keySubmitter.execute(tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test (expected = IllegalArgumentException.class)
  public void getExecutorForKeyFail() {
    distributor.getExecutorForKey(null);
  }

  @Test
  public void keyBasedSubmitterSubmitRunnableTest() throws InterruptedException, ExecutionException {
    KeyBasedSubmitterFactory ef = new KeyBasedSubmitterFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableTest(ef);
  }
  
  @Test
  public void keyBasedSubmitterSubmitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    KeyBasedSubmitterFactory ef = new KeyBasedSubmitterFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(ef);
  }
  
  @Test
  public void keyBasedSubmitterSubmitCallableTest() throws InterruptedException, ExecutionException {
    KeyBasedSubmitterFactory ef = new KeyBasedSubmitterFactory();
    
    SubmitterExecutorInterfaceTest.submitCallableTest(ef);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void keyBasedSubmitterSubmitRunnableFail() {
    KeyBasedSubmitterFactory ef = new KeyBasedSubmitterFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(ef);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void keyBasedSubmitterSubmitCallableFail() {
    KeyBasedSubmitterFactory ef = new KeyBasedSubmitterFactory();
    
    SubmitterExecutorInterfaceTest.submitCallableFail(ef);
  }
  
  @Test
  public void keyBasedSubmitterConsistentThreadTest() throws InterruptedException, TimeoutException {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        SubmitterExecutorInterface keySubmitter = distributor.getSubmitterForKey(key);
        keySubmitter.submit(tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000 * 20);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getSubmitterForKeyFail() {
    distributor.getSubmitterForKey(null);
  }
  
  @Test
  public void executeConsistentThreadTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.addTask(key, tdr);
      }
    });

    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
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
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.submitTask(key, tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableConsistentThreadTest() {
    List<TDCallable> runs = new ArrayList<TDCallable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to avoid execution till all are submitted
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        TDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDCallable tr = new TDCallable(tc, previous);
          runs.add(tr);
          distributor.submitTask(tc, tr);
          
          previous = tr;
        }
      }
    }
    
    Iterator<TDCallable> it = runs.iterator();
    while (it.hasNext()) {
      TDCallable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableFail() {
    try {
      distributor.submitTask(null, new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), (Callable<Object>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void executeStressTest() {
    final Object testLock = new Object();
    final int expectedCount = (PARALLEL_LEVEL * 2) * (RUNNABLE_COUNT_PER_LEVEL * 2);
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(expectedCount);
    
    // we can't use populate here because we don't want to hold the agentLock
    
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
        assertEquals(2, tr.getRunCount()); // verify each only ran twice
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
        assertFalse(tr.ranConcurrently());  // verify that it never run in parallel
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
  
  @Test
  public void taskExceptionTest() {
    Integer key = 1;
    UncaughtExceptionHandler originalUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    TestUncaughtExceptionHandler ueh = new TestUncaughtExceptionHandler();
    final RuntimeException testException = new RuntimeException();
    Thread.setDefaultUncaughtExceptionHandler(ueh);
    try {
      TestRunnable exceptionRunnable = new TestRuntimeFailureRunnable(testException);
      TestRunnable followRunnable = new TestRunnable();
      distributor.addTask(key, exceptionRunnable);
      distributor.addTask(key, followRunnable);
      exceptionRunnable.blockTillStarted();
      followRunnable.blockTillStarted();  // verify that it ran despite the exception
      assertTrue(ueh.getCalledWithThrowable() == testException);
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(originalUncaughtExceptionHandler);
    }
  }
  
  @Test
  public void limitExecutionPerCycleTest() {
    final AtomicInteger execCount = new AtomicInteger(0);
    TaskExecutorDistributor distributor = new TaskExecutorDistributor(1, new Executor() {
      @Override
      public void execute(Runnable command) {
        execCount.incrementAndGet();
        
        new Thread(command).start();
      }
    }, 1);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    
    distributor.addTask(this, btr);
    btr.blockTillStarted();
    
    // add second task while we know worker is active
    TestRunnable secondTask = new TestRunnable();
    distributor.addTask(this, secondTask);
    
    assertEquals(1, distributor.taskWorkers.size());
    assertEquals(1, distributor.taskWorkers.get(this).queue.size());
    
    btr.unblock();
    
    secondTask.blockTillFinished();
    
    // verify worker execed out between task
    assertEquals(2, execCount.get());
  }
  
  @Test
  public void limitExecutionPerCycleStressTest() {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(3, 3, 1000 * 10, 
                                                                              TaskPriority.High, 
                                                                              PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    final AtomicBoolean testComplete = new AtomicBoolean(false);
    try {
      final Integer key1 = 1;
      final Integer key2 = 2;
      Executor singleThreadedExecutor = scheduler.makeSubPool(1);
      final TaskExecutorDistributor distributor = new TaskExecutorDistributor(2, singleThreadedExecutor, 2);
      final AtomicInteger waitingTasks = new AtomicInteger();
      final AtomicReference<TestRunnable> lastTestRunnable = new AtomicReference<TestRunnable>();
      scheduler.execute(new Runnable() {  // execute thread to add for key 1
        @Override
        public void run() {
          while (! testComplete.get()) {
            TestRunnable next = new TestRunnable() {
              @Override
              public void handleRunStart() {
                waitingTasks.decrementAndGet();
                
                TestUtils.sleep(20);  // wait to make sure producer is faster than executor
              }
            };
            lastTestRunnable.set(next);
            waitingTasks.incrementAndGet();
            distributor.addTask(key1, next);
          }
        }
      });
      
      // block till there is for sure a backup of key1 tasks
      new TestCondition() {
        @Override
        public boolean get() {
          return waitingTasks.get() > 10;
        }
      }.blockTillTrue();
      
      TestRunnable key2Runnable = new TestRunnable();
      distributor.addTask(key2, key2Runnable);
      TestRunnable lastKey1Runnable = lastTestRunnable.get();
      key2Runnable.blockTillStarted();  // will throw exception if not started
      // verify it ran before the lastKey1Runnable
      assertFalse(lastKey1Runnable.ranOnce());
    } finally {
      testComplete.set(true);
      scheduler.shutdownNow();
    }
  }
  
  protected static class TDRunnable extends TestRunnable {
    protected final TDRunnable previousRunnable;
    protected final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    protected TDRunnable(ThreadContainer threadTracker, 
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
    
    public boolean previousRanFirst() {
      return previousRanFirst;
    }
  }
  
  protected static class TDCallable extends TestCallable {
    protected final TDCallable previousRunnable;
    protected final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    protected TDCallable(ThreadContainer threadTracker, 
                         TDCallable previousRunnable) {
      this.threadTracker = threadTracker;
      this.previousRunnable = previousRunnable;
      previousRanFirst = false;
      verifiedPrevious = false;
    }
    
    @Override
    public void handleCallStart() {
      threadTracker.running();
      
      if (! verifiedPrevious) {
        if (previousRunnable != null) {
          previousRanFirst = previousRunnable.isDone();
        } else {
          previousRanFirst = true;
        }
        
        verifiedPrevious = true;
      }
    }
    
    public boolean previousRanFirst() {
      return previousRanFirst;
    }
  }
  
  protected static class ThreadContainer {
    private Thread runningThread = null;
    private boolean threadConsistent = true;
    
    public synchronized void running() {
      if (runningThread == null) {
        runningThread = Thread.currentThread();
      } else {
        threadConsistent = threadConsistent && runningThread == Thread.currentThread();
      }
    }
    
    public boolean threadConsistent() {
      return threadConsistent;
    }
  }

  private class KeyBasedSubmitterFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private KeyBasedSubmitterFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize, 
                                                            boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                               1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      return new TaskExecutorDistributor(executor).getSubmitterForKey("foo");
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
  
  private interface AddHandler {
    public void addTDRunnable(Object key, TDRunnable tdr);
  }
}

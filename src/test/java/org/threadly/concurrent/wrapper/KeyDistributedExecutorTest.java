package org.threadly.concurrent.wrapper;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.concurrent.UnfairExecutor;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.concurrent.wrapper.limiter.ExecutorLimiter;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.SuppressedStackRuntimeException;
import org.threadly.util.TestExceptionHandler;

@SuppressWarnings("javadoc")
public class KeyDistributedExecutorTest extends ThreadlyTester {
  private static final int PARALLEL_LEVEL = TEST_QTY;
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY * 2;
  
  protected static UnfairExecutor executor;
  
  @BeforeClass
  public static void setupClass() {
    setIgnoreExceptionHandler();
    
    executor = new UnfairExecutor((PARALLEL_LEVEL * 2) + 1);
  }
  
  @AfterClass
  public static void cleanupClass() {
    executor.shutdownNow();
    executor = null;
  }
  
  protected Object agentLock;
  protected KeyDistributedExecutor distributor;
  
  @Before
  public void setup() {
    StripedLock sLock = new StripedLock(1);
    agentLock = sLock.getLock(null);  // there should be only one lock
    distributor = new KeyDistributedExecutor(executor, sLock, Integer.MAX_VALUE, false);
  }
  
  @After
  public void cleanup() {
    agentLock = null;
    distributor = null;
  }
  
  protected List<KDRunnable> populate(AddHandler ah) {
    final List<KDRunnable> runs = new ArrayList<>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to prevent execution till ready
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        KDRunnable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          KDRunnable tr = new KDRunnable(tc, previous);
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
    new KeyDistributedExecutor(executor);
    new KeyDistributedExecutor(executor, true);
    new KeyDistributedExecutor(executor, 1);
    new KeyDistributedExecutor(executor, 1, true);
    new KeyDistributedExecutor(1, executor);
    new KeyDistributedExecutor(1, executor, true);
    new KeyDistributedExecutor(1, executor, 1);
    new KeyDistributedExecutor(1, executor, 1, true);
    StripedLock sLock = new StripedLock(1);
    new KeyDistributedExecutor(executor, sLock, 1, false);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new KeyDistributedExecutor(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyDistributedExecutor(executor, null, 
                                 Integer.MAX_VALUE, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyDistributedExecutor(executor, new StripedLock(1), -1, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getExecutorTest() {
    assertTrue(executor == distributor.getExecutor());
  }
  
  @Test
  public void keyBasedSubmitterConsistentThreadTest() {
    List<KDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        SubmitterExecutor keySubmitter = distributor.getExecutorForKey(key);
        keySubmitter.submit(tdr);
      }
    });
    
    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      tr.blockTillFinished(1000 * 20);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getExecutorForKeyFail() {
    distributor.getExecutorForKey(null);
  }
  
  @Test
  public void executeFail() {
    try {
      distributor.execute(null, DoNothingRunnable.instance());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      distributor.execute(new Object(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void executeConsistentThreadTest() {
    List<KDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        distributor.execute(key, tdr);
      }
    });

    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitRunnableFail() {
    try {
      distributor.submit(null, DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), null, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableConsistentThreadTest() {
    List<KDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        distributor.submit(key, tdr);
      }
    });
    
    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableConsistentThreadTest() {
    List<KDCallable> runs = new ArrayList<>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to avoid execution till all are submitted
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        KDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          KDCallable tr = new KDCallable(tc, previous);
          runs.add(tr);
          distributor.submit(tc, tr);
          
          previous = tr;
        }
      }
    }
    
    Iterator<KDCallable> it = runs.iterator();
    while (it.hasNext()) {
      KDCallable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableFail() {
    try {
      distributor.submit(null, new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), (Callable<Void>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void executeStressTest() {
    final Object testLock = new Object();
    final int expectedCount = (PARALLEL_LEVEL * 2) * (RUNNABLE_COUNT_PER_LEVEL * 2);
    final List<KDRunnable> runs = new ArrayList<>(expectedCount);
    
    // we can't use populate here because we don't want to hold the agentLock
    
    executor.execute(new Runnable() {
      private final Map<Integer, ThreadContainer> containers = new HashMap<>();
      private final Map<Integer, KDRunnable> previousRunnables = new HashMap<>();
      
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
              
              KDRunnable tr = new KDRunnable(tc, previousRunnables.get(j)) {
                private boolean added = false;
                
                @Override
                public void handleRunFinish() {
                  if (! added) {
                    distributor.execute(threadTracker, this);
                    added = true;
                  }
                }
              };
              runs.add(tr);
              distributor.execute(tc, tr);
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
      Iterator<KDRunnable> it = runs.iterator();
      while (it.hasNext()) {
        KDRunnable tr = it.next();
        tr.blockTillFinished(20 * 1000, 2);
        assertEquals(2, tr.getRunCount()); // verify each only ran twice
        assertTrue(tr.previousRanFirst);  // verify runnables were run in order
        assertFalse(tr.ranConcurrently());  // verify that it never run in parallel
      }
    }
  }
  
  @Test
  public void taskExceptionTest() {
    Integer key = 1;
    TestExceptionHandler teh = new TestExceptionHandler();
    final RuntimeException testException = new SuppressedStackRuntimeException();
    ExceptionUtils.setDefaultExceptionHandler(teh);
    TestRunnable exceptionRunnable = new TestRuntimeFailureRunnable(testException);
    TestRunnable followRunnable = new TestRunnable();
    distributor.execute(key, exceptionRunnable);
    distributor.execute(key, followRunnable);
    exceptionRunnable.blockTillFinished();
    followRunnable.blockTillStarted();  // verify that it ran despite the exception
    
    assertEquals(1, teh.getCallCount());
    assertEquals(testException, teh.getLastThrowable());
  }
  
  @Test
  public void limitExecutionPerCycleTest() {
    final AtomicInteger execCount = new AtomicInteger(0);
    KeyDistributedExecutor distributor = new KeyDistributedExecutor(1, new Executor() {
      @Override
      public void execute(Runnable command) {
        execCount.incrementAndGet();
        
        new Thread(command).start();
      }
    }, 1);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    
    distributor.execute(this, btr);
    btr.blockTillStarted();
    
    // add second task while we know worker is active
    TestRunnable secondTask = new TestRunnable();
    distributor.execute(this, secondTask);
    
    assertEquals(1, distributor.taskWorkers.size());
    assertEquals(1, distributor.taskWorkers.get(this).queue.size());
    
    btr.unblock();
    
    secondTask.blockTillFinished();
    
    // verify worker execed out between task
    assertEquals(2, execCount.get());
  }
  
  @Test
  public void limitExecutionPerCycleStressTest() {
    PriorityScheduler scheduler = new StrictPriorityScheduler(3);
    final AtomicBoolean testComplete = new AtomicBoolean(false);
    try {
      final Integer key1 = 1;
      final Integer key2 = 2;
      Executor singleThreadedExecutor = new ExecutorLimiter(scheduler, 1);
      final KeyDistributedExecutor distributor = new KeyDistributedExecutor(2, singleThreadedExecutor, 2);
      final AtomicInteger waitingTasks = new AtomicInteger();
      final AtomicReference<TestRunnable> lastTestRunnable = new AtomicReference<>();
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
            distributor.execute(key1, next);
          }
        }
      });
      
      // block till there is for sure a backup of key1 tasks
      new TestCondition(() -> waitingTasks.get() > 10).blockTillTrue();
      
      TestRunnable key2Runnable = new TestRunnable();
      distributor.execute(key2, key2Runnable);
      TestRunnable lastKey1Runnable = lastTestRunnable.get();
      key2Runnable.blockTillStarted();  // will throw exception if not started
      // verify it ran before the lastKey1Runnable
      assertFalse(lastKey1Runnable.ranOnce());
    } finally {
      testComplete.set(true);
      scheduler.shutdownNow();
    }
  }
  
  private static void getTaskQueueSizeSimpleTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(new Executor() {
      @Override
      public void execute(Runnable command) {
        // kidding, don't actually execute, haha
      }
    }, accurateDistributor);
    
    assertEquals(0, kde.getTaskQueueSize(taskKey));
    
    kde.execute(taskKey, DoNothingRunnable.instance());
    
    // should add as first task
    assertEquals(1, kde.getTaskQueueSize(taskKey));
    
    kde.execute(taskKey, DoNothingRunnable.instance());
    
    // will now add into the queue
    assertEquals(2, kde.getTaskQueueSize(taskKey));
  }
  
  private static void getTaskQueueSizeThreadedTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(executor, accurateDistributor);
    
    assertEquals(0, kde.getTaskQueueSize(taskKey));
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    kde.execute(taskKey, btr);
    
    // add more tasks while remaining blocked
    kde.execute(taskKey, DoNothingRunnable.instance());
    kde.execute(taskKey, DoNothingRunnable.instance());
    
    btr.blockTillStarted();
    
    assertEquals(2, kde.getTaskQueueSize(taskKey));
    
    btr.unblock();
  }
  
  @Test
  public void getTaskQueueSizeInaccurateTest() {
    getTaskQueueSizeSimpleTest(false);
    getTaskQueueSizeThreadedTest(false);
  }
  
  @Test
  public void getTaskQueueSizeAccurateTest() {
    getTaskQueueSizeSimpleTest(true);
    getTaskQueueSizeThreadedTest(true);
  }
  
  private static void getTaskQueueSizeMapSimpleTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(new Executor() {
      @Override
      public void execute(Runnable command) {
        // kidding, don't actually execute, haha
      }
    }, accurateDistributor);
    
    Map<?, Integer> result = kde.getTaskQueueSizeMap();
    assertTrue(result.isEmpty());
    
    kde.execute(taskKey, DoNothingRunnable.instance());
    
    // should add as first task
    result = kde.getTaskQueueSizeMap();
    assertEquals(1, result.size());
    assertEquals((Integer)1, result.get(taskKey));
    
    kde.execute(taskKey, DoNothingRunnable.instance());
    
    // will now add into the queue
    result = kde.getTaskQueueSizeMap();
    assertEquals(1, result.size());
    assertEquals((Integer)2, result.get(taskKey));
  }
  
  private static void getTaskQueueSizeMapThreadedTest(boolean accurateDistributor) {
    final Object taskKey = new Object();
    KeyDistributedExecutor kde = new KeyDistributedExecutor(executor, accurateDistributor);

    Map<?, Integer> result = kde.getTaskQueueSizeMap();
    assertTrue(result.isEmpty());
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    kde.execute(taskKey, btr);
    
    // add more tasks while remaining blocked
    kde.execute(taskKey, DoNothingRunnable.instance());
    kde.execute(taskKey, DoNothingRunnable.instance());
    
    btr.blockTillStarted();
    
    result = kde.getTaskQueueSizeMap();
    assertEquals(1, result.size());
    assertEquals((Integer)2, result.get(taskKey));
    
    btr.unblock();
  }
  
  @Test
  public void getTaskQueueSizeMapInaccurateTest() {
    getTaskQueueSizeMapSimpleTest(false);
    getTaskQueueSizeMapThreadedTest(false);
  }
  
  @Test
  public void getTaskQueueSizeMapAccurateTest() {
    getTaskQueueSizeMapSimpleTest(true);
    getTaskQueueSizeMapThreadedTest(true);
  }
  
  public static class KDRunnable extends TestRunnable {
    public final KDRunnable previousRunnable;
    public final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    public KDRunnable(ThreadContainer threadTracker, KDRunnable previousRunnable) {
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
  
  public static class KDCallable extends TestCallable {
    public final KDCallable previousRunnable;
    public final ThreadContainer threadTracker;
    private volatile boolean previousRanFirst;
    private volatile boolean verifiedPrevious;
    
    public KDCallable(ThreadContainer threadTracker, KDCallable previousRunnable) {
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
  
  public static class ThreadContainer {
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
  
  protected interface AddHandler {
    public void addTDRunnable(Object key, KDRunnable tdr);
  }
}

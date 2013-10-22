package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestablePriorityScheduler;
import org.threadly.test.concurrent.TestablePriorityScheduler.OneTimeRunnable;

@SuppressWarnings("javadoc")
public class TestablePrioritySchedulerTest {
  private static final int RUNNABLE_COUNT = 100;
  private static final int THREAD_COUNT = 1000;
  
  private PriorityScheduledExecutor parentScheduler;
  private TestablePriorityScheduler testScheduler;
  
  @Before
  public void setup() {
    parentScheduler = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                    1000, TaskPriority.High, 200);
    testScheduler = new TestablePriorityScheduler(parentScheduler);
  }
  
  @After
  public void tearDown() {
    testScheduler = null;
    parentScheduler.shutdownNow();
    parentScheduler = null;
  }
  
  @Test
  public void executeTest() {
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      runnables.add(tr);
      testScheduler.execute(tr);
    }
    
    assertEquals(RUNNABLE_COUNT, testScheduler.tick()); // should execute all
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    assertEquals(0, testScheduler.tick()); // should not execute anything
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
  }
  
  @Test
  public void submitTest() {
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    List<Future<?>> futures = new ArrayList<Future<?>>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      runnables.add(tr);
      Future<?> future = testScheduler.submit(tr);
      assertNotNull(future);
      futures.add(future);
    }
    
    assertEquals(RUNNABLE_COUNT, testScheduler.tick()); // should execute all
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    assertEquals(0, testScheduler.tick()); // should not execute anything
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    Iterator<Future<?>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      assertTrue(futureIt.next().isDone());
    }
  }
  
  @Test
  public void scheduleExecuteTest() {
    long scheduleDelay = 1000 * 10;
    
    TestRunnable executeRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();
    
    testScheduler.schedule(scheduleRun, scheduleDelay);
    testScheduler.execute(executeRun);

    long startTime = System.currentTimeMillis();
    assertEquals(1, testScheduler.tick(startTime));

    assertEquals(1, executeRun.getRunCount());   // should have run
    assertEquals(0, scheduleRun.getRunCount());  // should NOT have run yet
    
    assertEquals(1, testScheduler.tick(startTime + scheduleDelay));
    
    assertEquals(1, executeRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should have run
    
    assertEquals(0, testScheduler.tick(startTime + scheduleDelay)); // should not execute anything
    
    assertEquals(1, executeRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void submitScheduledExecuteTest() {
    long scheduleDelay = 1000 * 10;
    
    TestRunnable submitRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();

    Future<?> future = testScheduler.submit(submitRun);
    assertNotNull(future);
    future = testScheduler.submitScheduled(scheduleRun, scheduleDelay);
    assertNotNull(future);

    long startTime = System.currentTimeMillis();
    assertEquals(1, testScheduler.tick(startTime));

    assertEquals(1, submitRun.getRunCount());   // should have run
    assertEquals(0, scheduleRun.getRunCount());  // should NOT have run yet
    
    assertEquals(1, testScheduler.tick(startTime + scheduleDelay));
    
    assertEquals(1, submitRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should have run
    
    assertEquals(0, testScheduler.tick(startTime + scheduleDelay)); // should not execute anything
    
    assertEquals(1, submitRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void recurringExecuteTest() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    testScheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    testScheduler.scheduleWithFixedDelay(initialDelay, delay, delay);

    long startTime = System.currentTimeMillis();
    assertEquals(1, testScheduler.tick(startTime));
    
    assertEquals(1, immediateRun.getRunCount());  // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet

    assertEquals(2, testScheduler.tick(startTime + delay));
    
    assertEquals(2, immediateRun.getRunCount());  // should have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run for the first time
    
    assertEquals(2, testScheduler.tick(startTime + (delay * 2)));
    
    assertEquals(3, immediateRun.getRunCount());  // should have run again
    assertEquals(2, initialDelay.getRunCount());  // should have run again
    
    assertEquals(0, testScheduler.tick(startTime + (delay * 2))); // should not execute anything
    
    assertEquals(3, immediateRun.getRunCount());  // should NOT have run again
    assertEquals(2, initialDelay.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void tickTimeNoProgressTest() {
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      testScheduler.execute(tr);
    }

    long now;
    assertEquals(RUNNABLE_COUNT, testScheduler.tick(now = System.currentTimeMillis())); // should execute all
    
    TestUtils.blockTillClockAdvances();
    
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      testScheduler.execute(tr);
    }
    
    assertEquals(RUNNABLE_COUNT, testScheduler.tick(now)); // should execute all again
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tickFail() {
    long now;
    testScheduler.tick(now = System.currentTimeMillis());
    
    testScheduler.tick(now - 1);
    fail("Exception should have been thrown");
  }
  
  @Test
  public void sleepThreadTest() {
    int sleepTime = 100;
    long now = System.currentTimeMillis();
    
    for (int i = 0; i < sleepTime; i++) {
      final SleepThread st = new SleepThread(i);
      testScheduler.execute(st);
      
      if (i == 0) {
        assertEquals(2, testScheduler.tick(now));
      
        assertEquals(0, testScheduler.tick(now));
      } else {
        assertEquals(1, testScheduler.tick(now));

        assertTrue(st.running);
        
        assertEquals(1, testScheduler.tick(now += i));
      }
    }
  }
  
  @Test
  public void waitWithoutNotifyThreadTest() {
    int waitTime = 100;
    long now = System.currentTimeMillis();
    
    for (int i = 0; i < waitTime; i++) {
      final WaitThread st = new WaitThread(i);
      testScheduler.execute(st);
      
      if (i == 0) {
        assertEquals(3, testScheduler.tick(now));
      
        assertEquals(0, testScheduler.tick(now));
      } else {
        assertEquals(1, testScheduler.tick(now));

        assertTrue(st.running);
        
        assertEquals(2, testScheduler.tick(now += i));
        
        assertTrue(st.wokenUp);
      }
    }
  }
  
  @Test
  public void waitAndNotifyThreadTest() {
    int runCount = RUNNABLE_COUNT / 5;  // we do less here because of the higher complexity for this test
    long now = System.currentTimeMillis();
    
    List<WaitThread> toWake = new ArrayList<WaitThread>((runCount / 2) + 1);
    List<WaitThread> ignored = new ArrayList<WaitThread>((runCount / 2) + 1);
    for (int i = 0; i < runCount; i++) {
      WaitThread wt = new WaitThread(Integer.MAX_VALUE);
      testScheduler.execute(wt);
      if (i % 2 == 0) {
        toWake.add(wt);
      } else {
        ignored.add(wt);
      }
    }
    
    assertEquals(runCount, testScheduler.tick(now));
    
    Iterator<WaitThread> it = toWake.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().running);
    }
    it = ignored.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().running);
    }
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(0, testScheduler.tick(now = System.currentTimeMillis()));
    
    it = toWake.iterator();
    while (it.hasNext()) {
      final VirtualLock lock = it.next().lock;
      testScheduler.execute(new Runnable() {
        @Override
        public void run() {
          if (lock.hashCode() % 2 == 0) {
            lock.signal();
          } else {
            lock.signalAll();
          }
        }
      });
    }
    
    assertEquals(toWake.size() * 2, testScheduler.tick(now = System.currentTimeMillis()));
    
    it = toWake.iterator(); // should have all been woken up
    while (it.hasNext()) {
      assertTrue(it.next().wokenUp);
    }
    it = ignored.iterator();  // should not be woken up
    while (it.hasNext()) {
      assertFalse(it.next().wokenUp);
    }
    
    // nothing more should be left to run
    assertEquals(0, testScheduler.tick(now = System.currentTimeMillis()));
  }
  
  @Test
  public void testAddAndGet() {
    long now = System.currentTimeMillis();
    testScheduler.tick(now);  // tick once so now matches
    
    int secondDelay = 10;
    OneTimeRunnable second = testScheduler.new OneTimeRunnable(new TestRunnable(), secondDelay, 
                                                               TaskPriority.High);
    testScheduler.add(second);
    assertEquals(1, testScheduler.taskQueue.size());

    int thirdDelay = secondDelay * 10;
    OneTimeRunnable third = testScheduler.new OneTimeRunnable(new TestRunnable(), thirdDelay, 
                                                              TaskPriority.High);
    testScheduler.add(third);
    assertEquals(2, testScheduler.taskQueue.size());
    assertEquals(second, testScheduler.taskQueue.get(0));
    assertEquals(third, testScheduler.taskQueue.get(1));

    OneTimeRunnable first = testScheduler.new OneTimeRunnable(new TestRunnable(), 0, 
                                                              TaskPriority.High);
    testScheduler.add(first);
    assertEquals(3, testScheduler.taskQueue.size());
    assertEquals(first, testScheduler.taskQueue.get(0));
    assertEquals(second, testScheduler.taskQueue.get(1));
    assertEquals(third, testScheduler.taskQueue.get(2));
    
    // should be returned in name order
    assertEquals(first, testScheduler.getNextTask());
    assertEquals(2, testScheduler.taskQueue.size());
    assertNull(testScheduler.getNextTask());
    assertEquals(2, testScheduler.taskQueue.size());
    testScheduler.updateTime(now += secondDelay);
    assertEquals(second, testScheduler.getNextTask());
    assertEquals(1, testScheduler.taskQueue.size());
    assertNull(testScheduler.getNextTask());
    assertEquals(1, testScheduler.taskQueue.size());
    testScheduler.updateTime(now += thirdDelay);
    assertEquals(third, testScheduler.getNextTask());
    assertEquals(0, testScheduler.taskQueue.size());
  }
  
  private class SleepThread extends TestRunnable {
    private final int sleepTime;
    private volatile boolean running = false;
    
    private SleepThread(int sleepTime) {
      this.sleepTime = sleepTime;
    }
    
    @Override
    public void handleRunStart() throws InterruptedException {
      running = true;
      
      sleep(sleepTime);
    }
    
    @Override
    public void handleRunFinish() {
      running = false;
    }
  }
  
  private class WaitThread extends TestRunnable {
    private final int waitTime;
    private volatile VirtualLock lock;
    private volatile boolean running = false;
    private volatile boolean wokenUp = false;
    
    private WaitThread(int waitTime) {
      this.waitTime = waitTime;
    }
    
    @Override
    public void handleRunStart() throws InterruptedException {
      lock = makeLock();
      running = true;
      synchronized (lock) {
        lock.await(waitTime);
      }
    }
    
    @Override
    public void handleRunFinish() {
      wokenUp = true;
      running = false;
    }
  }
}

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
    
    assertEquals(testScheduler.tick(), RUNNABLE_COUNT); // should execute all
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
    
    assertEquals(testScheduler.tick(), 0); // should not execute anything
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
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
    
    assertEquals(testScheduler.tick(), RUNNABLE_COUNT); // should execute all
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
    
    assertEquals(testScheduler.tick(), 0); // should not execute anything
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
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
    assertEquals(testScheduler.tick(startTime), 1);

    assertEquals(executeRun.getRunCount(), 1);   // should have run
    assertEquals(scheduleRun.getRunCount(), 0);  // should NOT have run yet
    
    assertEquals(testScheduler.tick(startTime + scheduleDelay), 1);
    
    assertEquals(executeRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should have run
    
    assertEquals(testScheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
    
    assertEquals(executeRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should NOT have run again
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
    assertEquals(testScheduler.tick(startTime), 1);

    assertEquals(submitRun.getRunCount(), 1);   // should have run
    assertEquals(scheduleRun.getRunCount(), 0);  // should NOT have run yet
    
    assertEquals(testScheduler.tick(startTime + scheduleDelay), 1);
    
    assertEquals(submitRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should have run
    
    assertEquals(testScheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
    
    assertEquals(submitRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should NOT have run again
  }
  
  @Test
  public void recurringExecuteTest() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    testScheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    testScheduler.scheduleWithFixedDelay(initialDelay, delay, delay);

    long startTime = System.currentTimeMillis();
    assertEquals(testScheduler.tick(startTime), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);  // should have run
    assertEquals(initialDelay.getRunCount(), 0);  // should NOT have run yet

    assertEquals(testScheduler.tick(startTime + delay), 2);
    
    assertEquals(immediateRun.getRunCount(), 2);  // should have run again
    assertEquals(initialDelay.getRunCount(), 1);  // should have run for the first time
    
    assertEquals(testScheduler.tick(startTime + (delay * 2)), 2);
    
    assertEquals(immediateRun.getRunCount(), 3);  // should have run again
    assertEquals(initialDelay.getRunCount(), 2);  // should have run again
    
    assertEquals(testScheduler.tick(startTime + (delay * 2)), 0); // should not execute anything
    
    assertEquals(immediateRun.getRunCount(), 3);  // should NOT have run again
    assertEquals(initialDelay.getRunCount(), 2);  // should NOT have run again
  }
  
  @Test
  public void tickTimeNoProgressTest() {
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      testScheduler.execute(tr);
    }

    long now;
    assertEquals(testScheduler.tick(now = System.currentTimeMillis()), RUNNABLE_COUNT); // should execute all
    
    TestUtils.blockTillClockAdvances();
    
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      testScheduler.execute(tr);
    }
    
    assertEquals(testScheduler.tick(now), RUNNABLE_COUNT); // should execute all again
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
        assertEquals(testScheduler.tick(now), 2);
      
        assertEquals(testScheduler.tick(now), 0);
      } else {
        assertEquals(testScheduler.tick(now), 1);

        assertTrue(st.running);
        
        assertEquals(testScheduler.tick(now += i), 1);
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
        assertEquals(testScheduler.tick(now), 3);
      
        assertEquals(testScheduler.tick(now), 0);
      } else {
        assertEquals(testScheduler.tick(now), 1);

        assertTrue(st.running);
        
        assertEquals(testScheduler.tick(now += i), 2);
        
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
    
    assertEquals(testScheduler.tick(now), runCount);
    
    Iterator<WaitThread> it = toWake.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().running);
    }
    it = ignored.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().running);
    }
    
    TestUtils.blockTillClockAdvances();
    
    assertEquals(testScheduler.tick(now = System.currentTimeMillis()), 0);
    
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
    
    assertEquals(testScheduler.tick(now = System.currentTimeMillis()), toWake.size() * 2);
    
    it = toWake.iterator(); // should have all been woken up
    while (it.hasNext()) {
      assertTrue(it.next().wokenUp);
    }
    it = ignored.iterator();  // should not be woken up
    while (it.hasNext()) {
      assertFalse(it.next().wokenUp);
    }
    
    // nothing more should be left to run
    assertEquals(testScheduler.tick(now = System.currentTimeMillis()), 0);
  }
  
  @Test
  public void testAddAndGet() {
    long now = System.currentTimeMillis();
    testScheduler.tick(now);  // tick once so now matches
    
    int secondDelay = 10;
    OneTimeRunnable second = testScheduler.new OneTimeRunnable(new TestRunnable(), secondDelay, 
                                                               TaskPriority.High);
    testScheduler.add(second);
    assertEquals(testScheduler.taskQueue.size(), 1);

    int thirdDelay = secondDelay * 10;
    OneTimeRunnable third = testScheduler.new OneTimeRunnable(new TestRunnable(), thirdDelay, 
                                                              TaskPriority.High);
    testScheduler.add(third);
    assertEquals(testScheduler.taskQueue.size(), 2);
    assertEquals(testScheduler.taskQueue.get(0), second);
    assertEquals(testScheduler.taskQueue.get(1), third);

    OneTimeRunnable first = testScheduler.new OneTimeRunnable(new TestRunnable(), 0, 
                                                              TaskPriority.High);
    testScheduler.add(first);
    assertEquals(testScheduler.taskQueue.size(), 3);
    assertEquals(testScheduler.taskQueue.get(0), first);
    assertEquals(testScheduler.taskQueue.get(1), second);
    assertEquals(testScheduler.taskQueue.get(2), third);
    
    // should be returned in name order
    assertEquals(testScheduler.getNextTask(), first);
    assertEquals(testScheduler.taskQueue.size(), 2);
    assertNull(testScheduler.getNextTask());
    assertEquals(testScheduler.taskQueue.size(), 2);
    testScheduler.updateTime(now += secondDelay);
    assertEquals(testScheduler.getNextTask(), second);
    assertEquals(testScheduler.taskQueue.size(), 1);
    assertNull(testScheduler.getNextTask());
    assertEquals(testScheduler.taskQueue.size(), 1);
    testScheduler.updateTime(now += thirdDelay);
    assertEquals(testScheduler.getNextTask(), third);
    assertEquals(testScheduler.taskQueue.size(), 0);
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

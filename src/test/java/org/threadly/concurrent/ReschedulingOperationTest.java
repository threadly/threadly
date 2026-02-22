package org.threadly.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class ReschedulingOperationTest extends ThreadlyTester {
  private static final int SCHEDULE_DELAY = 100;
  
  private TestableScheduler scheduler;
  
  @BeforeEach
  public void setup() {
    scheduler = new TestableScheduler();
  }
  
  @AfterEach
  public void cleanup() {
    scheduler = null;
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new TestReschedulingOperation(null, SCHEDULE_DELAY, false);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new TestReschedulingOperation(null, false);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new TestReschedulingOperation(scheduler, -1, false);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void runOnSchedulerAfterSignaledTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, SCHEDULE_DELAY, false);
    
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY));
    
    testOp.signalToRun();
    
    // should run once, but not again
    assertEquals(1, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(0, scheduler.getQueuedTaskCount());
    
    testOp.signalToRun();
    
    // should run again, but not again
    assertEquals(1, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(0, scheduler.getQueuedTaskCount());
    
    assertEquals(2, testOp.tr.getRunCount());
  }
  
  @Test
  public void runOnExecutorAfterSignaledTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, false);
    
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY));
    
    testOp.signalToRun();
    
    // should run once, but not again
    assertEquals(1, scheduler.tick());
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(0, scheduler.getQueuedTaskCount());
    
    testOp.signalToRun();
    
    // should run again, but not again
    assertEquals(1, scheduler.tick());
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(0, scheduler.getQueuedTaskCount());
    
    assertEquals(2, testOp.tr.getRunCount());
  }

  @Test
  public void autoRescheduleTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, SCHEDULE_DELAY, true);
    
    testOp.signalToRun();
    
    // should run every time
    assertEquals(1, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(1, scheduler.advance(SCHEDULE_DELAY));
    assertEquals(1, scheduler.getQueuedTaskCount());
    
    assertEquals(2, testOp.tr.getRunCount());
  }

  @Test
  public void changeScheduleDelayTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, SCHEDULE_DELAY, true);
    testOp.setScheduleDelay(SCHEDULE_DELAY / 2);
    
    testOp.signalToRun();

    assertEquals(1, scheduler.advance(SCHEDULE_DELAY / 2));
    assertEquals(1, testOp.tr.getRunCount());
  }

  @Test
  public void changeScheduleDelayFail() {
      assertThrows(UnsupportedOperationException.class, () -> {
      TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, false);
    
      testOp.setScheduleDelay(10);
      });
  }
  
  @Test
  public void signalToRunImmediatelyOnSchedulerTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, SCHEDULE_DELAY, false);

    testOp.signalToRunImmediately(false);

    assertEquals(1, scheduler.advance(1));
    assertEquals(1, testOp.tr.getRunCount());
  }
  
  @Test
  public void signalToRunImmediatelyOnExecutorTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, false);

    testOp.signalToRunImmediately(false);

    assertEquals(1, scheduler.advance(1));
    assertEquals(1, testOp.tr.getRunCount());
  }
  
  @Test
  public void signalToRunImmediatelyOnCallingThreadTest() {
    TestReschedulingOperation testOp = new TestReschedulingOperation(scheduler, SCHEDULE_DELAY, false);

    testOp.signalToRunImmediately(true);
    
    assertEquals(1, testOp.tr.getRunCount());
    assertEquals(0, scheduler.advance(SCHEDULE_DELAY)); // should have run in-thread not on scheduler
  }
  
  private static class TestReschedulingOperation extends ReschedulingOperation {
    public final TestRunnable tr = new TestRunnable();
    private final boolean alwaysReschedule;

    protected TestReschedulingOperation(Executor executor, boolean alwaysReschedule) {
      super(executor);
      
      this.alwaysReschedule = alwaysReschedule;
    }

    protected TestReschedulingOperation(SubmitterScheduler scheduler, 
                                        long scheduleDelay, boolean alwaysReschedule) {
      super(scheduler, scheduleDelay);
      
      this.alwaysReschedule = alwaysReschedule;
    }

    @Override
    protected void run() {
      tr.run();
      if (alwaysReschedule) {
        signalToRun();
      }
    }
  }
}

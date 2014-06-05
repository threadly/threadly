package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.concurrent.NoThreadScheduler.TaskContainer;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTaskContainerTest {
  private static NoThreadScheduler scheduler;
  
  @BeforeClass
  public static void setup() {
    scheduler = new NoThreadScheduler(false);
  }
  
  @AfterClass
  public static void tearDown() {
    scheduler = null;
  }
  
  @Test
  public void compareToTest() {
    TestContainer tc0 = new TestContainer(0, false);
    
    assertEquals(0, tc0.compareTo(tc0));
    assertEquals(0, tc0.compareTo(new TestContainer(0, false)));
    
    assertTrue(tc0.compareTo(new TestContainer(1, false)) < 0);
    assertTrue(tc0.compareTo(new TestContainer(-1, false)) > 0);
    
    Random r = new Random(Clock.lastKnownTimeMillis());
    List<TestContainer> testList = new ArrayList<TestContainer>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      testList.add(new TestContainer(r.nextInt(100), false));
    }
    
    Collections.sort(testList);
    
    int lastDelay = Integer.MIN_VALUE;
    Iterator<TestContainer> it = testList.iterator();
    while (it.hasNext()) {
      assertTrue(lastDelay <= (lastDelay = it.next().delay));
    }
  }
  
  @Test
  public void getContainedRunnableTest() {
    TestContainer tc = new TestContainer(0, false);
    
    assertTrue(tc.getContainedRunnable() == tc.runnable);
  }
  
  @Test
  public void runTaskTest() {
    TestContainer tc = new TestContainer(0, false);
    
    tc.runTask();
    
    assertTrue(tc.prepareCalled);
    assertTrue(tc.runCompleteCalled);
    assertTrue(((TestRunnable)tc.runnable).ranOnce());
  }
  
  @Test
  public void runTaskExceptionTest() {
    TestContainer tc = new TestContainer(0, true);
    
    try {
      tc.runTask();
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      // expected
    }
    
    assertTrue(tc.prepareCalled);
    assertTrue(tc.runCompleteCalled);
  }
  
  private class TestContainer extends TaskContainer {
    private final int delay;
    private boolean prepareCalled = false;
    private boolean runCompleteCalled = false;
    
    protected TestContainer(int delay, boolean throwException) {
      scheduler.super(throwException ? new TestRuntimeFailureRunnable() : new TestRunnable());
      
      this.delay = delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return delay;
    }

    @Override
    protected void prepareForRun() {
      assertFalse(prepareCalled);
      
      prepareCalled = true;
    }

    @Override
    protected void runComplete() {
      assertFalse(runCompleteCalled);
        
      runCompleteCalled = true;
    }

    @Override
    protected void setInitialDelay() {
      // ignored
    }
  }
}

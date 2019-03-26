package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import org.threadly.util.debug.Profiler.ThreadSample;

@SuppressWarnings("javadoc")
public class FilteredStackProfilerTest extends ProfilerTest {
  private static final int POLL_INTERVAL = 1;
  private static final int WAIT_TIME_FOR_COLLECTION = 50;

  @Before
  @Override
  public void setup() {
    profiler = new FilteredStackProfiler(".");
  }

  @Test
  @Override
  public void constructorTest() {
    int testPollInterval = Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS * 10;
    FilteredStackProfiler p;

    p = new FilteredStackProfiler("^org\\.threadly\\.");
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.pattern.matcher("org.threadly.SomeClass.run").find());
    assertFalse(p.filteredThreadStore.pattern.matcher("java.lang.Thread.run").find());

    p = new FilteredStackProfiler(Pattern.compile("^org\\.threadly\\."));
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.pattern.matcher("org.threadly.SomeClass.run").find());
    assertFalse(p.filteredThreadStore.pattern.matcher("java.lang.Thread.run").find());

    p = new FilteredStackProfiler(testPollInterval, "^org\\.threadly\\.");
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.pattern.matcher("org.threadly.SomeClass.run").find());
    assertFalse(p.filteredThreadStore.pattern.matcher("java.lang.Thread.run").find());

    p = new FilteredStackProfiler(testPollInterval, Pattern.compile("^org\\.threadly\\."));
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.pattern.matcher("org.threadly.SomeClass.run").find());
    assertFalse(p.filteredThreadStore.pattern.matcher("java.lang.Thread.run").find());
  }

  @Test
  public void getProfileThreadsIteratorEmptyTest() {
    FilteredStackProfiler profiler = new FilteredStackProfiler("^no matching stack frames$");
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();

    assertNotNull(it);
    assertFalse(it.hasNext());
  }

  @Test
  public void getProfileThreadsIteratorTest() {
    // This should only see this one thread executing this one particular test
    FilteredStackProfiler profiler = new FilteredStackProfiler(
      "^org\\.threadly\\.util\\.debug\\.FilteredStackProfilerTest\\.getProfileThreadsIteratorTest");
    Iterator<? extends ThreadSample> it = profiler.pStore.getProfileThreadsIterator();

    assertNotNull(it);
    assertTrue(it.hasNext());
    assertTrue(it.next().getThread() == Thread.currentThread());
    assertFalse(it.hasNext());
  }

  @Override
  @SuppressWarnings("unused")
  @Test(expected = IllegalArgumentException.class)
  public void constructorFail() {
    new FilteredStackProfiler(-1, ".");
  }
}

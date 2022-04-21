package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;

import org.threadly.util.debug.Profiler.ThreadSample;

@SuppressWarnings("javadoc")
public class FilteredStackProfilerTest extends ProfilerTest {
  @Before
  @Override
  public void setup() {
    profiler = new FilteredStackProfiler(POLL_INTERVAL, (p) -> startFutureResultSupplier.get(), ".");
    startFutureResultSupplier = profiler::dump;
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
    assertTrue(p.filteredThreadStore.filter.test(
                 new StackTraceElement[] { new StackTraceElement("org.threadly.SomeClass", "run", null, 42) }));
    assertFalse(p.filteredThreadStore.filter.test(
                  new StackTraceElement[] { new StackTraceElement("java.lang.Thread", "run", null, 456) }));

    p = new FilteredStackProfiler(stack -> Arrays.stream(stack).anyMatch(e -> e.getClassName().startsWith("org.threadly.")));
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.filter.test(
                 new StackTraceElement[] { new StackTraceElement("org.threadly.SomeClass", "run", null, 42) }));
    assertFalse(p.filteredThreadStore.filter.test(
                  new StackTraceElement[] { new StackTraceElement("java.lang.Thread", "run", null, 456) }));

    p = new FilteredStackProfiler(testPollInterval, "^org\\.threadly\\.");
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.filter.test(
                 new StackTraceElement[] { new StackTraceElement("org.threadly.SomeClass", "run", null, 42) }));
    assertFalse(p.filteredThreadStore.filter.test(
                  new StackTraceElement[] { new StackTraceElement("java.lang.Thread", "run", null, 456) }));

    p = new FilteredStackProfiler(
      testPollInterval, stack -> Arrays.stream(stack).anyMatch(e -> e.getClassName().startsWith("org.threadly.")));
    assertNotNull(p.filteredThreadStore.threadTraces);
    assertTrue(p.filteredThreadStore.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.filteredThreadStore.pollIntervalInMs);
    assertNull(p.filteredThreadStore.collectorThread.get());
    assertNull(p.filteredThreadStore.dumpingThread);
    assertNotNull(p.startStopLock);
    assertTrue(p.filteredThreadStore.filter.test(
                 new StackTraceElement[] { new StackTraceElement("org.threadly.SomeClass", "run", null, 42) }));
    assertFalse(p.filteredThreadStore.filter.test(
                  new StackTraceElement[] { new StackTraceElement("java.lang.Thread", "run", null, 456) }));
  }

  @Test
  public void getProfileThreadsIteratorEmptyTest() {
    FilteredStackProfiler profiler = new FilteredStackProfiler("^no matching stack frames$");
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();

    assertNotNull(it);
    assertFalse(it.hasNext());
  }

  @Override
  @Test
  public void getProfileThreadsIteratorTest() {
    // This should only see this one thread executing this one particular test
    FilteredStackProfiler profiler = new FilteredStackProfiler(
      "^(app//)?org\\.threadly\\.util\\.debug\\.FilteredStackProfilerTest\\.getProfileThreadsIteratorTest");
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

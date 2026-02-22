package org.threadly.util.debug;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.statistics.PrioritySchedulerStatisticTracker;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandler;

@SuppressWarnings("javadoc")
public class ProfilerTest extends ThreadlyTester {
  protected static final int POLL_INTERVAL = 1;
  private static final int MIN_RESPONSE_LENGTH = 10;
  
  protected Profiler profiler;
  protected Supplier<String> startFutureResultSupplier;
  
  @BeforeEach
  public void setup() {
    profiler = new Profiler(POLL_INTERVAL, (p) -> startFutureResultSupplier.get());
    startFutureResultSupplier = profiler::dump;
  }
  
  @AfterEach
  public void cleanup() {
    profiler.stop();
    profiler = null;
  }
  
  protected void profilingExecutor(@SuppressWarnings("unused") Executor executor) {
    // ignored by default, overriden in other cases
  }
  
  protected void blockForProfilerSample() {
    int startCount = profiler.getCollectedSampleQty();
    new TestCondition(() -> profiler.getCollectedSampleQty() > startCount).blockTillTrue(1000 * 20);
  }
  
  @Test
  public void constructorTest() {
    int testPollInterval = Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS * 10;
    Profiler p;
    
    p = new Profiler();
    assertNotNull(p.pStore.threadTraces);
    assertEquals(0, p.pStore.threadTraces.size());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.pStore.pollIntervalInMs);
    assertNull(p.pStore.collectorThread.get());
    assertNull(p.pStore.dumpingThread);
    assertNotNull(p.startStopLock);
    
    p = new Profiler(testPollInterval);
    assertNotNull(p.pStore.threadTraces);
    assertEquals(0, p.pStore.threadTraces.size());
    assertEquals(testPollInterval, p.pStore.pollIntervalInMs);
    assertNull(p.pStore.collectorThread.get());
    assertNull(p.pStore.dumpingThread);
    assertNotNull(p.startStopLock);
  }
  
  @Test
  public void getProfileThreadsIteratorTest() {
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertTrue(it.hasNext());
    assertNotNull(it.next());
  }
  
  @Test
  public void profileThreadsIteratorNextFail() {
      assertThrows(NoSuchElementException.class, () -> {
      Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
    
      while (it.hasNext()) {
        assertNotNull(it.next());
      }
    
      it.next();
      });
  }
  
  @Test
  public void profileThreadsIteratorRemoveFail() {
      assertThrows(UnsupportedOperationException.class, () -> {
      Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
      it.next();
    
      // not currently supported
      it.remove();
      });
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new Profiler(-1);
      });
  }
  
  @Test
  public void isRunningTest() {
    assertFalse(profiler.isRunning());
    
    /* verification of isRunning after start happens in 
     * startWithoutExecutorTest and startWitExecutorTest
     */
  }
  
  @Test
  public void startWithoutExecutorTest() {
    profiler.start(null);
    
    assertTrue(profiler.isRunning());
    blockForProfilerSample();
  }
  
  @Test
  public void startWithExecutorTest() {
    PrioritySchedulerStatisticTracker e = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(0, e.getActiveTaskCount());
      
      profiler.start(e);
      
      assertTrue(profiler.isRunning());
      assertEquals(1, e.getActiveTaskCount());
      blockForProfilerSample();
    } finally {
      profiler.stop();
      e.shutdownNow();
    }
  }
  
  @Test
  public void startWithSameThreadExecutorTest() throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    PrioritySchedulerStatisticTracker s = new PrioritySchedulerStatisticTracker(1);
    try {
      s.schedule(() -> {
        av.assertTrue(profiler.isRunning());
        try {
          blockForProfilerSample();
          profiler.stop();  // this should unblock the test thread
        } catch (Exception e) {
          av.fail(e);
        }
        av.signalComplete();
      }, 200);
      profiler.start(SameThreadSubmitterExecutor.instance()); // will block while profile runs
      av.waitForTest();  // test already completed, just check result
    } finally {
      s.shutdownNow();
    }
  }
  
  @Test
  public void startWithSameThreadExecutorAndTimeoutTest() {
    profiler.start(SameThreadSubmitterExecutor.instance(), 200);
    assertFalse(profiler.isRunning());
    assertTrue(profiler.getCollectedSampleQty() > 0);
  }
  
  @Test
  public void startWithTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    long start = Clock.accurateForwardProgressingMillis();
    ListenableFuture<String> lf = profiler.start(DELAY_TIME);
    String result = lf.get(DELAY_TIME + (10 * 1000), TimeUnit.MILLISECONDS);
    long end = Clock.accurateForwardProgressingMillis();

    // profiler should be stopped now
    assertFalse(profiler.isRunning());
    assertTrue(end - start >= DELAY_TIME);
    assertNotNull(result);
  }
  
  @Test
  public void startWitExecutorAndTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    StrictPriorityScheduler ps = new StrictPriorityScheduler(2);
    try {
      long start = Clock.accurateForwardProgressingMillis();
      ListenableFuture<String> lf = profiler.start(ps, DELAY_TIME);
      String result = lf.get(10 * 1000, TimeUnit.MILLISECONDS);
      long end = Clock.accurateForwardProgressingMillis();

      // profiler should be stopped now
      assertFalse(profiler.isRunning());
      assertTrue(end - start >= DELAY_TIME);
      assertNotNull(result);
    } finally {
      ps.shutdownNow();
    }
  }
  
  @Test
  public void startFutureResultSupplierOverrideTest() throws InterruptedException, ExecutionException {
    startFutureResultSupplier = () -> null;
    ListenableFuture<String> lf = profiler.start(SameThreadSubmitterExecutor.instance(), 10);
    assertNull(lf.get());
  }

  @Test
  public void stopTwiceTest() {
    ListenableFuture<String> lf = profiler.start(20_000);
    profiler.stop();
    assertTrue(lf.isDone());
    lf = profiler.start(20_000);
    profiler.stop();
    assertTrue(lf.isDone());
  }
  
  @Test
  public void getAndSetProfileIntervalTest() {
    int TEST_VAL = 100;
    profiler.setPollInterval(TEST_VAL);
    
    assertEquals(TEST_VAL, profiler.getPollInterval());
  }
  
  @Test
  public void setProfileIntervalFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      profiler.setPollInterval(-1);
      });
  }
  
  @Test
  public void resetTest() {
    profiler.start();
    // verify there are some samples
    blockForProfilerSample();
    final Thread runningThread = profiler.pStore.collectorThread.get();
    profiler.stop();
    
    // verify stopped
    new TestCondition(() -> ! runningThread.isAlive()).blockTillTrue(1000 * 20);
    
    profiler.reset();
    
    assertEquals(0, profiler.pStore.threadTraces.size());
    assertEquals(0, profiler.getCollectedSampleQty());
  }
  
  @Test
  public void dumpStoppedStringTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStoppedOutputStreamTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpOutputStreamTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringOnlySummaryTest() {
    profiler.start();
    blockForProfilerSample();
    
    assertTrue(profiler.dump(false, 1).startsWith("Combined profile for all threads"));
  }
  
  protected static void verifyDumpStr(String resultStr) {
    assertTrue(resultStr.length() > MIN_RESPONSE_LENGTH);
    
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_COUNT_HEADER));
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_NET_HEADER));
  }
  
  private void verifyDumpContains(String str) {
    new TestCondition(() -> profiler.dump(false, 1), (s) -> s.contains(str)).blockTillTrue();
  }
  
  @Test
  public void idlePrioritySchedulerTest() {
    PriorityScheduler ps = new PriorityScheduler(2);
    profilingExecutor(ps);
    ps.prestartAllThreads();
    profiler.start();
    blockForProfilerSample();
    
    new TestCondition(() -> profiler.dump(false, 1), 
                      (s) -> s.contains("PriorityScheduler idle thread (stack 1)") &&
                             s.contains("PriorityScheduler idle thread (stack 2)"))
        .blockTillTrue();
  }
  
  @Test
  public void idleSingleThreadSchedulerTest() throws InterruptedException, ExecutionException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    profilingExecutor(sts);
    sts.prestartExecutionThread(true);
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("SingleThreadScheduler idle thread (stack 1)");
    
    sts.schedule(DoNothingRunnable.instance(), 600_000);
    sts.submit(DoNothingRunnable.instance()).get();
    blockForProfilerSample();

    verifyDumpContains("SingleThreadScheduler idle thread (stack 2)");
  }
  
  @Test
  public void idlePrioritySchedulerWithExceptionHandlerTest() {
    PriorityScheduler ps = new PriorityScheduler(2, null, 100, 
                                                 new ConfigurableThreadFactory(ExceptionHandler.PRINT_STACKTRACE_HANDLER));
    profilingExecutor(ps);
    ps.prestartAllThreads();
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("PriorityScheduler with ExceptionHandler idle thread (stack 1)");
    verifyDumpContains("PriorityScheduler with ExceptionHandler idle thread (stack 2)");
  }
  
  @Test
  public void idleSingleThreadSchedulerWithExceptionHandlerTest() throws InterruptedException, ExecutionException {
    SingleThreadScheduler sts = new SingleThreadScheduler(new ConfigurableThreadFactory(ExceptionHandler.PRINT_STACKTRACE_HANDLER));
    profilingExecutor(sts);
    sts.prestartExecutionThread(true);
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("SingleThreadScheduler with ExceptionHandler idle thread (stack 1)");
    
    sts.schedule(DoNothingRunnable.instance(), 600_000);
    sts.submit(DoNothingRunnable.instance()).get();
    blockForProfilerSample();

    verifyDumpContains("SingleThreadScheduler with ExceptionHandler idle thread (stack 2)");
  }
  
  @Test
  public void idleThreadPoolExecutorSynchronousQueueTest() {
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, 
                                                    new SynchronousQueue<>());
    profilingExecutor(tpe);
    tpe.prestartCoreThread();
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("ThreadPoolExecutor SynchronousQueue idle thread");
  }
  
  @Test
  public void idleThreadPoolExecutorArrayBlockingQueueTest() {
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, 
                                                    new ArrayBlockingQueue<>(1));
    profilingExecutor(tpe);
    tpe.prestartCoreThread();
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("ThreadPoolExecutor ArrayBlockingQueue idle thread");
  }
  
  @Test
  public void idleThreadPoolExecutorLinkedBlockingQueueTest() {
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, 
                                                    new LinkedBlockingQueue<>());
    profilingExecutor(tpe);
    tpe.prestartCoreThread();
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("ThreadPoolExecutor LinkedBlockingQueue idle thread");
  }
  
  @Test
  public void idleScheduledThreadPoolExecutorTest() {
    ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1);
    profilingExecutor(stpe);
    stpe.prestartCoreThread();
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("ScheduledThreadPoolExecutor idle thread");
  }
  
  @Test
  public void idleForkJoinPoolTest() {
    ForkJoinPool fjp = new ForkJoinPool(1, (pool) -> {
      ForkJoinWorkerThread t = 
          new ForkJoinWorkerThread(pool) { /* nothing added, need protected visibility */ };
      t.setDaemon(true);
      return t;
    }, null, false);
    
    profilingExecutor((r) -> fjp.invoke(ForkJoinTask.adapt(r)));
    fjp.invoke(ForkJoinTask.adapt(DoNothingRunnable.instance()));
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("ForkJoinPool idle thread");
  }
  
  @Test
  public void idleAsyncForkJoinPoolTest() {
    ForkJoinPool fjp = new ForkJoinPool(1, (pool) -> {
      ForkJoinWorkerThread t = 
          new ForkJoinWorkerThread(pool) { /* nothing added, need protected visibility */ };
      t.setDaemon(true);
      return t;
    }, null, true);

    profilingExecutor((r) -> fjp.invoke(ForkJoinTask.adapt(r)));
    fjp.invoke(ForkJoinTask.adapt(DoNothingRunnable.instance()));
    profiler.start();
    blockForProfilerSample();

    verifyDumpContains("ForkJoinPool idle thread");
  }
}

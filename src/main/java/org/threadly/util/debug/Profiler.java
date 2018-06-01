package org.threadly.util.debug;

import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE1;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE2;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER1;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER2;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_PRIORITY_SCHEDULE1;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_PRIORITY_SCHEDULE2;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR1;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR2;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER1;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER2;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_ARRAY_QUEUE;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_LINKED_QUEUE;
import static org.threadly.util.debug.CommonStacktraces.IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_SYNCHRONOUS_QUEUE;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;

/**
 * Tool for profiling a running java application to get an idea of where the slow points 
 * (either because of lock contention, or because of high computational demand).
 * <p>
 * This tool definitely incurs some load within the system, so it should only be used while 
 * debugging, and not as general use.  In addition if it is left running without being reset, it 
 * will continue to consume more and more memory.
 * 
 * @since 1.0.0
 */
public class Profiler {
  protected static final short DEFAULT_POLL_INTERVAL_IN_MILLIS = 100;
  protected static final short NUMBER_TARGET_LINE_LENGTH = 8;
  protected static final String FUNCTION_BY_NET_HEADER;
  protected static final String FUNCTION_BY_COUNT_HEADER;
  private static final short DEFAULT_MAP_INITIAL_SIZE = 16;
  
  static {
    String prefix = "functions by ";
    String columns = "(total, top, name)";
    FUNCTION_BY_NET_HEADER = prefix + "top count: " + columns;
    FUNCTION_BY_COUNT_HEADER = prefix + "total count: " + columns;
    
    CommonStacktraces.init();
  }
  
  protected final Object startStopLock;
  protected final ProfileStorage pStore;
  protected final List<WeakReference<SettableListenableFuture<String>>> stopFutures; // guarded by startStopLock
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@link #dump()} with a provided output stream to get the results to.  
   * <p>
   * This uses a default poll interval of 100 milliseconds.
   */
  public Profiler() {
    this(DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@link #dump()} with a provided output stream to get the results to.
   * 
   * @param pollIntervalInMs frequency to check running threads
   */
  public Profiler(int pollIntervalInMs) {
    this(new ProfileStorage(pollIntervalInMs));
  }
  
  /**
   * This constructor allows extending classes to provide their own implementation of the 
   * {@link ProfileStorage}.  Ultimately all constructors will default to this one.
   * 
   * @param outputFile file to dump results to on stop (or {@code null} to not dump on stop)
   * @param pStore Storage to be used for holding profile results and getting the Iterator for threads
   */
  protected Profiler(ProfileStorage pStore) {
    this.startStopLock = new Object();
    this.pStore = pStore;
    this.stopFutures = new ArrayList<>(2);
  }
  
  /**
   * Change how long the profiler waits before getting additional thread stacks.  This value must 
   * be greater than or equal to 0.
   * 
   * @param pollIntervalInMs time in milliseconds to wait between thread data dumps
   */
  public void setPollInterval(int pollIntervalInMs) {
    ArgumentVerifier.assertNotNegative(pollIntervalInMs, "pollIntervalInMs");
    
    this.pStore.pollIntervalInMs = pollIntervalInMs;
  }
  
  /**
   * Call to get the currently set profile interval.  This is the amount of time the profiler 
   * waits between collecting thread data.
   * 
   * @return returns the profile interval in milliseconds
   */
  public int getPollInterval() {
    return pStore.pollIntervalInMs;
  }
  
  /**
   * Call to get an estimate on how many times the profiler has collected a sample of the thread 
   * stacks.  This number may be lower than the actual sample quantity, but should never be 
   * higher.  It can be used to ensure a minimum level of accuracy from within the profiler.
   * 
   * @return the number of times since the start or last reset we have sampled the threads
   */
  public int getCollectedSampleQty() {
    return pStore.collectedSamples.intValue();
  }
  
  /**
   * Reset the current stored statistics.  The statistics will continue to grow in memory until 
   * the profiler is either stopped, or until this is called.
   */
  public void reset() {
    pStore.threadTraces.clear();
    pStore.collectedSamples.reset();
  }
  
  /**
   * Call to check weather the profile is currently running/started.
   * 
   * @return {@code true} if there is a thread currently collecting statistics.
   */
  public boolean isRunning() {
    return pStore.collectorThread.get() != null;
  }
  
  /**
   * Starts the profiler running in a new thread.  
   * <p>
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.
   */
  public void start() {
    start(null, -1, null);
  }
  
  /**
   * Starts the profiler running in a new thread.  
   * <p>
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.
   * <p>
   * If an executor is provided, this call will block until the the profiler has been started on 
   * the provided executor.
   * 
   * @param executor executor to execute on, or {@code null} if new thread should be created
   */
  public void start(Executor executor) {
    start(executor, -1, null);
  }
  
  /**
   * Starts the profiler running in a new thread.  
   * <p>
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.  
   * <p>
   * If {@code sampleDurationInMillis} is greater than zero the Profiler will invoke 
   * {@link #stop()} in that many milliseconds.  
   * <p>
   * The returned {@link ListenableFuture} will be provided the dump when {@link #stop()} is 
   * invoked next.  Either from a timeout provided to this call, or a manual invocation of 
   * {@link #stop()}.
   * 
   * @param sampleDurationInMillis if greater than {@code 0} the profiler will only run for this many milliseconds
   * @return Future that will be completed with the dump string when the profiler is stopped
   */
  public ListenableFuture<String> start(long sampleDurationInMillis) {
    return start(null, sampleDurationInMillis);
  }
  
  /**
   * Starts the profiler running in a new thread.  
   * <p>
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.  
   * <p>
   * If an executor is provided, this call will block until the the profiler has been started on 
   * the provided executor.
   * <p>
   * If {@code sampleDurationInMillis} is greater than zero the Profiler will invoke 
   * {@link #stop()} in that many milliseconds.
   * <p>
   * The returned {@link ListenableFuture} will be provided the dump when {@link #stop()} is 
   * invoked next.  Either from a timeout provided to this call, or a manual invocation of 
   * {@link #stop()}.
   * 
   * @param executor executor to execute on, or {@code null} if new thread should be created
   * @param sampleDurationInMillis if greater than {@code 0} the profiler will only run for this many milliseconds
   * @return Future that will be completed with the dump string when the profiler is stopped
   */
  public ListenableFuture<String> start(Executor executor, long sampleDurationInMillis) {
    SettableListenableFuture<String> result = new SettableListenableFuture<>();
    start(executor, sampleDurationInMillis, result);
    return result;
  }
  
  /**
   * The ultimate start call for the profiler.  This handles all possible start permutations.  
   * If an {@link Executor} is provided, that will be used to run the profiler thread.  If there 
   * is a duration provided > 0, a thread will be started to perform profiler shutdown.  If 
   * {@code sampleDurationInMillis} is greater than {@code 0} and the profiler is already running, 
   * it will be stopped and restarted.
   * 
   * @param executor Executor or scheduler to use for execution as possible
   * @param sampleDurationInMillis if greater than {@code 0} an automatic stop will occur after that many milliseconds 
   * @param completionFuture If not {@code null} future will be called once the next {@link #stop()} is invoked
   */
  private void start(Executor executor, final long sampleDurationInMillis, 
                     SettableListenableFuture<String> completionFuture) {
    final ProfilerRunner pr = new ProfilerRunner(pStore);
    boolean runInCallingThread = false;
    Thread callingThread = null;
    synchronized (startStopLock) {
      if (sampleDurationInMillis > 0) {
        // stop in case it's running, this allows us to simplify our start logic
        stop();
      }
      if (completionFuture != null) {
        stopFutures.add(new WeakReference<>(completionFuture));
      }
      if (pStore.collectorThread.get() == null) {
        if (executor == null) {
          // no executor, so we simply create our own thread
          Thread thread = new Thread(pr);
          
          pStore.collectorThread.set(thread);
          
          thread.setName("Threadly Profiler data collector");
          thread.setPriority(Thread.MAX_PRIORITY);
          thread.start();
        } else if (executor == SameThreadSubmitterExecutor.instance() || 
                   executor instanceof SameThreadSubmitterExecutor) {
          // I don't really like having special logic if the goal is to profile on the invoking 
          // thread like this, but anything else might complicate the logic below (or at least it's 
          // not obvious what a clean way to adjust the below logic is).  Despite the use of an 
          // AtomicReference for the collectorThread reference, if we allow the thread to be set 
          // outside of the lock we risk a case where the profiler was started, started, then 
          // stopped.  If the second start task was able to submit to the pool, but not execute till 
          // after the stop, then a profiler would continue to run (maybe forever)  
          callingThread = Thread.currentThread();
          pStore.collectorThread.set(callingThread);
          runInCallingThread = true;
        } else {
          final SettableListenableFuture<?> runningThreadFuture = new SettableListenableFuture<>();
          
          executor.execute(new ExecutorRunnerTask(pStore, runningThreadFuture, pr));
          
          // now block till collectorThread has been set and profiler has started on the executor
          try {
            runningThreadFuture.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (ExecutionException e) {
            throw ExceptionUtils.makeRuntime(e.getCause());
          }
        }
        // start or schedule to handle run time limit
        if (sampleDurationInMillis > 0) {
          pStore.dumpLoopRun = new Runnable() {
            private final long startTime = Clock.accurateForwardProgressingMillis();
            
            @Override
            public void run() {
              if (Clock.lastKnownForwardProgressingMillis() - startTime > sampleDurationInMillis) {
                pStore.dumpLoopRun = null;
                stop();
              }
            }
          };
        }
      }
    }
    if (runInCallingThread) {
      int origPriority = callingThread.getPriority();
      try {
        if (origPriority < Thread.MAX_PRIORITY) {
          callingThread.setPriority(Thread.MAX_PRIORITY);
        }
        pr.run();
      } finally {
        if (origPriority < Thread.MAX_PRIORITY) {
          callingThread.setPriority(origPriority);
        }
      }
      Thread.interrupted(); // reset interrupted status incase that is what caused us to unblock
    }
  }
  
  /**
   * Stops the profiler from collecting more statistics.  If a file was provided at construction, 
   * the results will be written to that file.  It is possible to request the results using the 
   * {@link #dump()} call after it has stopped.
   */
  public void stop() {
    synchronized (startStopLock) {
      Thread runningThread = pStore.collectorThread.get();
      if (runningThread != null) {
        runningThread.interrupt();
        pStore.collectorThread.set(null);
        
        if (! stopFutures.isEmpty()) {
          String result = null;
          Iterator<WeakReference<SettableListenableFuture<String>>> it = stopFutures.iterator();
          while (it.hasNext()) {
            SettableListenableFuture<String> slf = it.next().get();
            if (slf != null) {
              if (result == null) {
                result = dump();
              }
              slf.setResult(result);
            }
          }
          stopFutures.clear();
        }
      }
    }
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @return The dumped results as a single String
   */
  public String dump() {
    return dump(true);
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @return The dumped results as a single String
   * @param dumpIndividualThreads If {@code true} then a report of stacks seen for individual threads is also dumped
   */
  public String dump(boolean dumpIndividualThreads) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    dump(new BufferedOutputStream(baos), dumpIndividualThreads);
    
    return baos.toString();
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @param out OutputStream to write results to
   */
  public void dump(OutputStream out) {
    dump(out, true);
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @param out OutputStream to write results to
   * @param dumpIndividualThreads If {@code true} then a report of stacks seen for individual threads is also dumped
   */
  public void dump(OutputStream out, boolean dumpIndividualThreads) {
    dump(new PrintStream(out, false), dumpIndividualThreads);
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @param ps PrintStream to write results to
   */
  public void dump(PrintStream ps) {
    dump(ps, true);
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @param ps PrintStream to write results to
   * @param dumpIndividualThreads If {@code true} then a report of stacks seen for individual threads is also dumped
   */
  public void dump(PrintStream ps, boolean dumpIndividualThreads) {
    pStore.dumpingThread = Thread.currentThread();
    try {
      Map<Trace, Integer> globalTraces = new HashMap<>();
      // create a local copy so the stats wont change while we are dumping them
      // convert to a pair list so we can sort by name
      List<Pair<ThreadIdentifier, ThreadSamples>> threadSamples = Pair.convertMap(pStore.threadTraces);
      Collections.sort(threadSamples, 
                       (p1, p2) -> p1.getRight().threadNames().compareTo(p2.getRight().threadNames()));
      Iterator<Pair<ThreadIdentifier, ThreadSamples>> it = threadSamples.iterator();
      while (it.hasNext()) {
        Pair<ThreadIdentifier, ThreadSamples> entry = it.next();
        if (dumpIndividualThreads) {
          ps.println("Profile for thread: " + 
                        entry.getRight().threadNames() + ';' + entry.getLeft().threadId);
          dumpTraces(entry.getRight().traceSet(), null, ps);
        }
        
        // add in this threads trace data to the global trace map
        Iterator<Trace> traceIt = entry.getRight().traceSet().iterator();
        while (traceIt.hasNext()) {
          globalTraces.compute(traceIt.next(), 
                               (k, v) -> v == null ? k.getThreadCount() : v + k.getThreadCount());
        }

        if (dumpIndividualThreads) {
          ps.println("--------------------------------------------------");
          ps.println();
        }
      }
        
      // log out global data
      if (globalTraces.size() > 1 || ! dumpIndividualThreads) {
        ps.println("Combined profile for all threads....");
        dumpTraces(globalTraces.keySet(), globalTraces, ps);
      }
      
      ps.flush();
    } finally {
      pStore.dumpingThread = null;
    }
  }
  
  /**
   * Dumps the traces in the provided set to the provided output stream.
   * 
   * @param traces Set to examine traces to dump statistics about
   * @param globalCount {@code true} to examine the global counts of the traces
   * @param out Output to dump results to
   */
  private static void dumpTraces(Set<Trace> traces, 
                                 final Map<Trace, Integer> globalCounts, PrintStream out) {
    Map<Function, Function> methods = new HashMap<>();
    Trace[] traceArray = traces.toArray(new Trace[traces.size()]);
    int total = 0;
    int nativeCount = 0;
    
    for (Trace t: traceArray) {
      if (globalCounts != null) {
        total += globalCounts.get(t);
      } else {
        total += t.getThreadCount();
      }
      
      if (t.elements.length > 0 && t.elements[0].isNativeMethod()) {
        if (globalCounts != null) {
          nativeCount += globalCounts.get(t);
        } else {
          nativeCount += t.getThreadCount();
        }
      }
      
      for (int i = 0; i < t.elements.length; ++i) {
        Function n = new Function(t.elements[i].getClassName(), t.elements[i].getMethodName());
        Function f = methods.get(n);
        if (f == null) {
          methods.put(n, n);
          f = n;
        }
        if (globalCounts != null) {
          f.incrementCount(globalCounts.get(t), i > 0);
        } else {
          f.incrementCount(t.getThreadCount(), i > 0);
        }
      }
    }
    
    Function[] methodArray = methods.keySet().toArray(new Function[methods.size()]);
    
    out.println(" total count: " + StringUtils.padStart(Integer.toString(total), 
                                                        NUMBER_TARGET_LINE_LENGTH, ' '));
    out.println("native count: " + StringUtils.padStart(Integer.toString(nativeCount), 
                                                        NUMBER_TARGET_LINE_LENGTH, ' '));
    
    out.println();
    out.println(FUNCTION_BY_NET_HEADER);
    out.println();
    
    Arrays.sort(methodArray, (a, b) -> b.getStackTopCount() - a.getStackTopCount());
    
    for (int i = 0; i < methodArray.length; i++) {
      dumpFunction(methodArray[i], out);
    }
    
    out.println();
    out.println(FUNCTION_BY_COUNT_HEADER);
    out.println();
    
    Arrays.sort(methodArray, (a, b) -> b.getCount() - a.getCount());
    
    for (int i = 0; i < methodArray.length; i++) {
      dumpFunction(methodArray[i], out);
    }
    
    out.println();
    out.println("traces by count:");
    out.println();
    
    if (globalCounts != null) {
      Arrays.sort(traceArray, (a, b) -> globalCounts.get(b) - globalCounts.get(a));
    } else {
      Arrays.sort(traceArray, (a, b) -> b.getThreadCount() - a.getThreadCount());
    }
    
    for (int i = 0; i < traceArray.length; i++) {
      Trace t = traceArray[i];
      int count;
      if (globalCounts != null) {
        count = globalCounts.get(t);
      } else {
        count = t.getThreadCount();
      }
      out.println(count + " time(s):");

      if (IDLE_THREAD_TRACE_PRIORITY_SCHEDULE1.equals(t)) {
        out.println("\tPriorityScheduler idle thread (stack 1)\n");
      } else if (IDLE_THREAD_TRACE_PRIORITY_SCHEDULE2.equals(t)) {
        out.println("\tPriorityScheduler idle thread (stack 2)\n");
      } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE1.equals(t)) {
        out.println("\tPriorityScheduler with ExceptionHandler idle thread (stack 1)\n");
      } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE2.equals(t)) {
        out.println("\tPriorityScheduler with ExceptionHandler idle thread (stack 2)\n");
      } else if (IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER1.equals(t)) {
        out.println("\tSingleThreadScheduler idle thread (stack 1)\n");
      } else if (IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER2.equals(t)) {
        out.println("\tSingleThreadScheduler idle thread (stack 2)\n");
      } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER1.equals(t)) {
        out.println("\tSingleThreadScheduler with ExceptionHandler idle thread (stack 1)\n");
      } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER2.equals(t)) {
        out.println("\tSingleThreadScheduler with ExceptionHandler idle thread (stack 2)\n");
      } else if (IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_SYNCHRONOUS_QUEUE.equals(t)) {
        out.println("\tThreadPoolExecutor SynchronousQueue idle thread\n");
      } else if (IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_ARRAY_QUEUE.equals(t)) {
        out.println("\tThreadPoolExecutor ArrayBlockingQueue idle thread\n");
      } else if (IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_LINKED_QUEUE.equals(t)) {
        out.println("\tThreadPoolExecutor LinkedBlockingQueue idle thread\n");
      } else if (IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR1.equals(t)) {
        out.println("\tScheduledThreadPoolExecutor idle thread (stack 1)\n");
      } else if (IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR2.equals(t)) {
        out.println("\tScheduledThreadPoolExecutor idle thread (stack 2)\n");
      } else {
        out.println(ExceptionUtils.stackToString(t.elements));
      }
    }
  }
  
  /**
   * Dumps the output for a given function to the provided PrintStream.
   * 
   * @param f Function to format for
   * @param out PrintStream to print out to
   */
  private static void dumpFunction(Function f, PrintStream out) {
    out.print(StringUtils.padStart(Integer.toString(f.getCount()), 
                                   NUMBER_TARGET_LINE_LENGTH, ' '));
    out.print(StringUtils.padStart(Integer.toString(f.getStackTopCount()), 
                                   NUMBER_TARGET_LINE_LENGTH, ' '));
    out.print(' ');
    out.print(f.className);
    out.print('.');
    out.println(f.function);
  }
  
  @Override
  protected void finalize() {
    // stop collection thread if running so that stored data can be GC'ed
    stop();
  }
  
  /**
   * A small interface to represent and provide access to details for a sampled thread.
   * 
   * @since 3.8.0
   */
  protected interface ThreadSample {
    /**
     * Get the reference to the thread which is to be sampled.  No attempt should be made to get 
     * the stack trace directly from that thread, instead {@link #getStackTrace()} will be used.
     * 
     * @return Thread reference that the provided stack trace originated from
     */
    public Thread getThread();
    
    /**
     * Returns the stack trace to be used for sampling.  This stack trace may have been generated 
     * previously (for example at constructor), or may have been generated lazily at the time of 
     * invocation.
     * 
     * @return Array of stack trace elements representing the threads state
     */
    public StackTraceElement[] getStackTrace();
  }
  
  /**
   * An iterator which will enumerate all the running threads within the VM.  It is expected that 
   * this iterator is NOT called in parallel.  This is also a single use object, once iteration is 
   * complete it should be allowed to be garbage collected.
   * 
   * @since 3.4.0
   */
  protected static class ThreadIterator implements Iterator<ThreadSample> {
    protected final Iterator<Map.Entry<Thread, StackTraceElement[]>> it;
    
    protected ThreadIterator() {
      it = Thread.getAllStackTraces().entrySet().iterator();
    }
    
    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public ThreadSample next() {
      final Map.Entry<Thread, StackTraceElement[]> entry = it.next();
      return new ThreadSample() {
        @Override
        public Thread getThread() {
          return entry.getKey();
        }
        
        @Override
        public StackTraceElement[] getStackTrace() {
          return entry.getValue();
        }
      };
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  /**
   * Collection of classes and data structures for data used in profiling threads.  This 
   * represents the shared memory between the collection thread and the threads which 
   * start/stop/dump the profiler statistics.
   * 
   * @since 3.5.0
   */
  protected static class ProfileStorage {
    protected final AtomicReference<Thread> collectorThread;
    protected final Map<ThreadIdentifier, ThreadSamples> threadTraces;
    protected final LongAdder collectedSamples;
    protected volatile int pollIntervalInMs;
    protected volatile Thread dumpingThread;
    protected volatile Runnable dumpLoopRun;
    
    public ProfileStorage(int pollIntervalInMs) {
      ArgumentVerifier.assertNotNegative(pollIntervalInMs, "pollIntervalInMs");
      
      collectorThread = new AtomicReference<>(null);
      threadTraces = new ConcurrentHashMap<>(DEFAULT_MAP_INITIAL_SIZE);
      collectedSamples = new LongAdder();
      this.pollIntervalInMs = pollIntervalInMs;
      dumpingThread = null;
      dumpLoopRun = null;
    }
    
    /**
     * A small call to get an iterator of threads that should be examined for this profiler cycle.  
     * <p>
     * This is a protected call, so it can be overridden to implement other profilers that want to 
     * control which threads are being profiled.
     * <p>
     * It is guaranteed that this will be called in a single threaded manner.
     * 
     * @return an {@link Iterator} of {@link ThreadSample} to examine and add data for our profile
     */
    protected Iterator<? extends ThreadSample> getProfileThreadsIterator() {
      return new ThreadIterator();
    }
  }
  
  /**
   * Class for executing the {@ilnk ProfilerRunner} on a {@link Executor}.  This normally would be 
   * a lambda or anonymous inner class.  But having it defined like this ensures we don't hold a 
   * reference to our parent class (allowing finalization stops to work correctly).
   * 
   * @since 5.25
   */
  protected static class ExecutorRunnerTask implements Runnable {
    private final ProfileStorage pStore;
    private final SettableListenableFuture<?> runningThreadFuture;
    private final ProfilerRunner pr;
    
    public ExecutorRunnerTask(ProfileStorage pStore, 
                              SettableListenableFuture<?> runningThreadFuture, 
                              ProfilerRunner pr) {
      this.pStore = pStore;
      this.runningThreadFuture = runningThreadFuture;
      this.pr = pr;
    }
    
    @Override
    public void run() {
      Thread currentThread = Thread.currentThread();
      try {
        // if collector thread can't be set, then some other thread has taken over
        if (! pStore.collectorThread.compareAndSet(null, currentThread)) {
          return;
        }
      } finally {
        runningThreadFuture.setResult(null);
      }
      
      String originalName = currentThread.getName();
      int origPriority = currentThread.getPriority();
      try {
        if (origPriority < Thread.MAX_PRIORITY) {
          currentThread.setPriority(Thread.MAX_PRIORITY);
        }
        currentThread.setName("Threadly Profiler data collector[" + originalName + "]");
        pr.run();
      } finally {
        if (origPriority < Thread.MAX_PRIORITY) {
          currentThread.setPriority(origPriority);
        }
        currentThread.setName(originalName);
      }
    }
    
  }
  
  /**
   * Class which runs, collecting statistics for the profiler to later analyze.
   * 
   * @since 1.0.0
   */
  protected static class ProfilerRunner implements Runnable {
    private final ProfileStorage pStore;
    
    protected ProfilerRunner(ProfileStorage pStore) {
      this.pStore = pStore;
    }
    
    @Override
    public void run() {
      Thread runningThread = Thread.currentThread();
      while (pStore.collectorThread.get() == runningThread) {
        boolean storedSample = false;
        Iterator<? extends ThreadSample> it = pStore.getProfileThreadsIterator();
        while (it.hasNext()) {
          ThreadSample threadSample = it.next();
          
          // we skip the Profiler threads (collector thread, and dumping thread if one exists)
          if (threadSample.getThread() != runningThread & 
              threadSample.getThread() != pStore.dumpingThread) {
            StackTraceElement[] threadStack = threadSample.getStackTrace();
            if (threadStack.length > 0) {
              storedSample = true;
              
              pStore.threadTraces
                    .computeIfAbsent(new ThreadIdentifier(threadSample.getThread()), 
                                     (k) -> new ThreadSamples())
                    .recordSample(new Trace(threadStack), threadSample.getThread().getName());
            }
          }
        }
        
        if (storedSample) {
          pStore.collectedSamples.increment();
        }
        try {
          Thread.sleep(pStore.pollIntervalInMs);
        } catch (InterruptedException e) {
          pStore.collectorThread.compareAndSet(runningThread, null);
          Thread.currentThread().interrupt(); // reset status
          return;
        }
        Runnable toRun = pStore.dumpLoopRun;
        if (toRun != null) {
          try {
            toRun.run();
          } catch (Throwable t) {
            ExceptionUtils.handleException(t);
          }
        }
      }
    }
  }
  
  /**
   * Storage for samples from a thread.  This can include more than just the stack traces (for 
   * example the thread names as well).
   * 
   * @since 5.25
   */
  protected static class ThreadSamples {
    private final Map<Trace, Trace> traces = new ConcurrentHashMap<>(DEFAULT_MAP_INITIAL_SIZE);
    private final Set<String> threadNames = ConcurrentHashMap.newKeySet(1);
    private volatile String cachedThreadNames = null;
    
    public void recordSample(Trace trace, String threadName) {
      if (threadNames.add(threadName)) {
        cachedThreadNames = null;
      }
      
      Trace existingTrace = traces.get(trace);
      if (existingTrace == null) {
        // this is really cheap to de-duplicate, so might as well
        if (IDLE_THREAD_TRACE_PRIORITY_SCHEDULE1.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_PRIORITY_SCHEDULE1.elements);
        } else if (IDLE_THREAD_TRACE_PRIORITY_SCHEDULE2.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_PRIORITY_SCHEDULE2.elements);
        } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE1.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE1.elements);
        } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE2.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_EXCEPTION_HANDLER_PRIORITY_SCHEDULE2.elements);
        } else if (IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER1.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER1.elements);
        } else if (IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER2.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_SINGLE_THREAD_SCHEDULER2.elements);
        } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER1.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER1.elements);
        } else if (IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER2.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_EXCEPTION_HANDLER_SINGLE_THREAD_SCHEDULER2.elements);
        } else if (IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_SYNCHRONOUS_QUEUE.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_SYNCHRONOUS_QUEUE.elements);
        } else if (IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_ARRAY_QUEUE.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_ARRAY_QUEUE.elements);
        } else if (IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_LINKED_QUEUE.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_THREAD_POOL_EXECUTOR_LINKED_QUEUE.elements);
        } else if (IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR1.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR1.elements);
        } else if (IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR2.equals(trace)) {
          trace = new Trace(IDLE_THREAD_TRACE_SCHEDULED_THREAD_POOL_EXECUTOR2.elements);
        }
        
        traces.put(trace, trace);
      } else {
        existingTrace.incrementThreadCount();
      }
    }

    public String threadNames() {
      String cachedThreadNames = this.cachedThreadNames;
      if (cachedThreadNames != null) {
        return cachedThreadNames;
      } else if (threadNames.size() == 1) {
        return this.cachedThreadNames = threadNames.iterator().next();
      } else if (threadNames.isEmpty()) {
        throw new IllegalStateException("No samples recorded for thread");
      } else {
        // we want names in a consistent order
        List<String> nameCopy = new ArrayList<>(threadNames);
        Collections.sort(nameCopy);
        return this.cachedThreadNames = nameCopy.toString();
      }
    }

    public Set<Trace> traceSet() {
      return traces.keySet();
    }
  }
  
  /**
   * Small class to store referencing to thread.  This is designed to be more memory efficient 
   * than just using a string.  It also tries to delay the expensive storage aspect until we know 
   * we know we will need to store it long term.
   * 
   * @since 4.9.0
   */
  protected static class ThreadIdentifier {
    private final long threadId;
    private final int hashCode;
    
    /**
     * Construct a new identifier which can be used for hash and equals comparison.
     * 
     * @param t Thread to be referenced
     */
    public ThreadIdentifier(Thread t) {
      this.threadId = t.getId();
      this.hashCode = t.hashCode();
    }
    
    @Override
    public String toString() {
      return "ThreadId:" + threadId;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else {
        try {
          ThreadIdentifier t = (ThreadIdentifier)o;
          return t.threadId == threadId && t.hashCode == hashCode;
        } catch (ClassCastException e) {
          return false;
        }
      }
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
  }
  
  /**
   * Class which represents a stack trace.  The is used so we can track how many times a given 
   * stack is seen.
   * <p>
   * This stack trace reference will be specific to a single thread.
   * 
   * @since 1.0.0
   */
  protected static class Trace extends ComparableTrace {
    /* threadSeenCount is how many times this trace has been seen in a specific thread.  It should 
     * only be incremented by a single thread, but can be read from any thread.
     */
    private volatile int threadSeenCount = 1;
    
    public Trace(StackTraceElement[] elements) {
      super(elements);
    }
    
    /**
     * Increments the internally tracked thread seen count by one.
     */
    protected void incrementThreadCount() {
      // this should only be incremented from a single thread
      threadSeenCount++;
    }
    
    /**
     * Getter for the current thread seen count.
     * 
     * @return a result of how many times {@link #incrementThreadCount()} has been called
     */
    protected int getThreadCount() {
      return threadSeenCount;
    }
  }
  
  /**
   * Class to represent a specific function call.  This is used so we can track how many times we 
   * see a given function.
   * 
   * @since 1.0.0
   */
  protected static class Function {
    protected final String className;
    protected final String function;
    protected final int hashCode;
    private int count;
    private int childCount;
    
    public Function(String className, String funtion) {
      this.className = className;
      this.function = funtion;
      this.hashCode = className.hashCode() ^ function.hashCode();
    }
    
    /**
     * Increments the internal stored seen count.
     * 
     * @param count amount to increment count by
     * @param child {@code true} if the stack position not the head position
     */
    protected void incrementCount(int count, boolean child) {
      this.count += count;
      if (child) {
        childCount += count;
      }
    }
    
    /**
     * Get the total count this function has been seen.
     * 
     * @return returns the value summed from calls to {@link #incrementCount(int, boolean)}
     */
    protected int getCount() {
      return count;
    }
    
    /**
     * Returns the number of times this function has been seen as the top of the stack.  This 
     * value is incremented when {@link #incrementCount(int, boolean)} is called with a 
     * {@code false}.
     *  
     * @return The summed value of this function seen as the top of the stack
     */
    protected int getStackTopCount() {
      return count - childCount;
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else {
        try {
          Function m = (Function) o;
          return m.hashCode == hashCode && 
                   m.className.equals(className) && 
                   m.function.equals(function);
        } catch (ClassCastException e) {
          return false;
        }
      }
    }
  }
}

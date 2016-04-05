package org.threadly.util.debug;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;

/**
 * <p>Tool for profiling a running java application to get an idea of where the slow points 
 * (either because of lock contention, or because of high computational demand).</p>
 * 
 * <p>This tool definitely incurs some load within the system, so it should only be used while 
 * debugging, and not as general use.  In addition if it is left running without being reset, it 
 * will continue to consume more and more memory.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class Profiler {
  protected static final short DEFAULT_POLL_INTERVAL_IN_MILLIS = 100;
  protected static final short NUMBER_TARGET_LINE_LENGTH = 6;
  protected static final String FUNCTION_BY_NET_HEADER;
  protected static final String FUNCTION_BY_COUNT_HEADER;
  private static final short DEFAULT_MAP_INITIAL_SIZE = 16;
  private static final float DEFAULT_MAP_LOAD_FACTOR = .75f;
  private static final short DEFAULT_MAP_CONCURRENCY_LEVEL = 2;
  
  static {
    String prefix = "functions by ";
    String columns = "(total, top, name)";
    FUNCTION_BY_NET_HEADER = prefix + "top count: " + columns;
    FUNCTION_BY_COUNT_HEADER = prefix + "total count: " + columns;
  }
  
  protected final Object startStopLock;
  protected final ProfileStorage pStore;
  protected final List<SettableListenableFuture<String>> stopFutures; // guarded by startStopLock
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@link #dump()} with a provided output stream to get the results to.  
   * 
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
    this.stopFutures = new ArrayList<SettableListenableFuture<String>>(2);
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
    return pStore.collectedSamples.get();
  }
  
  /**
   * Reset the current stored statistics.  The statistics will continue to grow in memory until 
   * the profiler is either stopped, or until this is called.
   */
  public void reset() {
    pStore.threadTraces.clear();
    pStore.collectedSamples.set(0);
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
   * 
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.
   */
  public void start() {
    start(null, -1, null);
  }
  
  /**
   * Starts the profiler running in a new thread.  
   * 
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.
   * 
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
   * 
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.  
   * 
   * If {@code sampleDurationInMillis} is greater than zero the Profiler will invoke 
   * {@link #stop()} in that many milliseconds.  
   * 
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
   * 
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.  
   * 
   * If an executor is provided, this call will block until the the profiler has been started on 
   * the provided executor.
   * 
   * If {@code sampleDurationInMillis} is greater than zero the Profiler will invoke 
   * {@link #stop()} in that many milliseconds.
   * 
   * The returned {@link ListenableFuture} will be provided the dump when {@link #stop()} is 
   * invoked next.  Either from a timeout provided to this call, or a manual invocation of 
   * {@link #stop()}.
   * 
   * @param executor executor to execute on, or {@code null} if new thread should be created
   * @param sampleDurationInMillis if greater than {@code 0} the profiler will only run for this many milliseconds
   * @return Future that will be completed with the dump string when the profiler is stopped
   */
  public ListenableFuture<String> start(Executor executor, long sampleDurationInMillis) {
    SettableListenableFuture<String> result = new SettableListenableFuture<String>();
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
    synchronized (startStopLock) {
      if (sampleDurationInMillis > 0) {
        // stop in case it's running, this allows us to simplify our start logic
        stop();
      }
      if (completionFuture != null) {
        stopFutures.add(completionFuture);
      }
      if (pStore.collectorThread.get() == null) {
        final ProfilerRunner pr = new ProfilerRunner(pStore);
        if (executor == null) {
          // no executor, so we simply create our own thread
          Thread thread = new Thread(pr);
          
          pStore.collectorThread.set(thread);
          
          thread.setName("Profiler data collector");
          thread.setPriority(Thread.MAX_PRIORITY);
          thread.start();
        } else {
          final SettableListenableFuture<?> runningThreadFuture;
          runningThreadFuture = new SettableListenableFuture<Void>();
          
          executor.execute(new Runnable() {
            @Override
            public void run() {
              try {
                // if collector thread can't be set, then some other thread has taken over
                if (! pStore.collectorThread.compareAndSet(null, Thread.currentThread())) {
                  return;
                }
              } finally {
                runningThreadFuture.setResult(null);
              }
              
              pr.run();
            }
          });
          
          // now block till collectorThread has been set and profiler has started on the executor
          try {
            runningThreadFuture.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (ExecutionException e) {
            // is virtually impossible
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
        
        String result = null;
        if (! stopFutures.isEmpty()) {
          result = dump();
          Iterator<SettableListenableFuture<String>> it = stopFutures.iterator();
          while (it.hasNext()) {
            it.next().setResult(result);
          }
        }
      }
    }
  }
  
  /**
   * Creates an identifier to represent the thread, a combination of the name, and the id.
   * 
   * @param t Thread to build identifier for
   * @return String to represent this thread uniquely
   */
  private static String getThreadIdentifier(Thread t) {
    return t.toString() + ';' + Long.toHexString(t.getId());
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
      Map<Trace, Integer> globalTraces = new HashMap<Trace, Integer>();
      // create a local copy so the stats wont change while we are dumping them
      Map<String, Map<Trace, Trace>> threadTraces = new HashMap<String, Map<Trace, Trace>>(pStore.threadTraces);
      
      // log out individual thread traces
      Iterator<Entry<String, Map<Trace, Trace>>> it = threadTraces.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, Map<Trace, Trace>> entry = it.next();
        if (dumpIndividualThreads) {
          ps.println("Profile for thread: " + entry.getKey());
          dumpTraces(entry.getValue().keySet(), null, ps);
        }
        
        // add in this threads trace data to the global trace map
        Iterator<Trace> traceIt = entry.getValue().keySet().iterator();
        while (traceIt.hasNext()) {
          Trace currTrace = traceIt.next();
          Integer globalTraceCount = globalTraces.get(currTrace);
          if (globalTraceCount == null) {
            // make sure this is reset in case we dump multiple times
            globalTraces.put(currTrace, currTrace.getThreadCount());
          } else {
            // update the global count
            globalTraces.put(currTrace, currTrace.getThreadCount() + globalTraceCount);
          }
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
                                 final Map<Trace, Integer> globalCounts, 
                                 PrintStream out) {
    Map<Function, Function> methods = new HashMap<Function, Function>();
    Trace[] traceArray = traces.toArray(new Trace[traces.size()]);
    int total = 0;
    int nativeCount = 0;
    
    for (Trace t: traceArray) {
      if (globalCounts != null) {
        total += globalCounts.get(t);
      } else {
        total += t.getThreadCount();
      }
      
      if (t.elements.length > 0 && 
          t.elements[0].isNativeMethod()) {
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
    
    Arrays.sort(methodArray, new Comparator<Function>() {
      @Override
      public int compare(Function a, Function b) {
        return b.getStackTopCount() - a.getStackTopCount();
      }
    });
    
    for (int i = 0; i < methodArray.length; i++) {
      dumpFunction(methodArray[i], out);
    }
    
    out.println();
    out.println(FUNCTION_BY_COUNT_HEADER);
    out.println();
    
    Arrays.sort(methodArray, new Comparator<Function>() {
      @Override
      public int compare(Function a, Function b) {
        return b.getCount() - a.getCount();
      }
    });
    
    for (int i = 0; i < methodArray.length; i++) {
      dumpFunction(methodArray[i], out);
    }
    
    out.println();
    out.println("traces by count:");
    out.println();
    
    if (globalCounts != null) {
      Arrays.sort(traceArray, new Comparator<Trace>() {
        @Override
        public int compare(Trace a, Trace b) {
          return globalCounts.get(b) - globalCounts.get(a);
        }
      });
    } else {
      Arrays.sort(traceArray, new Comparator<Trace>() {
        @Override
        public int compare(Trace a, Trace b) {
          return b.getThreadCount() - a.getThreadCount();
        }
      });
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
      
      out.println(ExceptionUtils.stackToString(t.elements));
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
   * <p>A small interface to represent and provide access to details for a sampled thread.</p>
   * 
   * @author jent - Mike Jensen
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
   * <p>An iterator which will enumerate all the running threads within the VM.  It is expected 
   * that this iterator is NOT called in parallel.  This is also a single use object, once 
   * iteration is complete it should be allowed to be garabage collected.</p>
   * 
   * @author jent - Mike Jensen
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
   * <p>Collection of classes and data structures for data used in profiling threads.  This 
   * represents the shared memory between the collection thread and the threads which 
   * start/stop/dump the profiler statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.5.0
   */
  protected static class ProfileStorage {
    protected final AtomicReference<Thread> collectorThread;
    protected final Map<String, Map<Trace, Trace>> threadTraces;
    protected final AtomicInteger collectedSamples;
    protected volatile int pollIntervalInMs;
    protected volatile Thread dumpingThread;
    protected volatile Runnable dumpLoopRun;
    
    public ProfileStorage(int pollIntervalInMs) {
      ArgumentVerifier.assertNotNegative(pollIntervalInMs, "pollIntervalInMs");
      
      collectorThread = new AtomicReference<Thread>(null);
      threadTraces = new ConcurrentHashMap<String, Map<Trace, Trace>>(DEFAULT_MAP_INITIAL_SIZE, 
                                                                      DEFAULT_MAP_LOAD_FACTOR, 
                                                                      DEFAULT_MAP_CONCURRENCY_LEVEL);
      collectedSamples = new AtomicInteger(0);
      this.pollIntervalInMs = pollIntervalInMs;
      dumpingThread = null;
      dumpLoopRun = null;
    }
    
    /**
     * A small call to get an iterator of threads that should be examined for this profiler cycle.  
     * 
     * This is a protected call, so it can be overridden to implement other profilers that want to 
     * control which threads are being profiled.
     * 
     * It is guaranteed that this will be called in a single threaded manner.
     * 
     * @return an {@link Iterator} of {@link ThreadSample} to examine and add data for our profile
     */
    protected Iterator<? extends ThreadSample> getProfileThreadsIterator() {
      return new ThreadIterator();
    }
  }
  
  /**
   * <p>Class which runs, collecting statistics for the profiler to later analyze.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  private static class ProfilerRunner implements Runnable {
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
          if (threadSample.getThread() != runningThread && 
              threadSample.getThread() != pStore.dumpingThread) {
            StackTraceElement[] threadStack = threadSample.getStackTrace();
            if (threadStack.length > 0) {
              storedSample = true;
              String threadIdentifier = getThreadIdentifier(threadSample.getThread());
              Trace t = new Trace(threadStack);
              
              Map<Trace, Trace> existingTraces = pStore.threadTraces.get(threadIdentifier);
              if (existingTraces == null) {
                existingTraces = new ConcurrentHashMap<Trace, Trace>(DEFAULT_MAP_INITIAL_SIZE, 
                                                                     DEFAULT_MAP_LOAD_FACTOR, 
                                                                     DEFAULT_MAP_CONCURRENCY_LEVEL);
                pStore.threadTraces.put(threadIdentifier, existingTraces);
  
                existingTraces.put(t, t);
              } else {
                Trace existingTrace = existingTraces.get(t);
                if (existingTrace == null) {
                  existingTraces.put(t, t);
                } else {
                  existingTrace.incrementThreadCount();
                }
              }
            }
          }
        }
        
        if (storedSample) {
          pStore.collectedSamples.incrementAndGet();
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
          ExceptionUtils.runRunnable(toRun);
        }
      }
    }
  }
  
  /**
   * <p>Class which represents a stack trace.  The is used so we can track how many times a given 
   * stack is seen.</p>
   * 
   * <p>This stack trace reference will be specific to a single thread.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class Trace implements Comparable<Trace> {
    protected final StackTraceElement[] elements;
    protected final int hash;
    /* threadSeenCount is how many times this trace has been seen in a specific thread.  It should 
     * only be incremented by a single thread, but can be read from any thread.
     */
    private volatile int threadSeenCount = 1;
    
    public Trace(StackTraceElement[] elements) {
      this.elements = elements;
      
      int h = 0;
      for (StackTraceElement e: elements) {
        h ^= e.hashCode();
      }
      hash = h;
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
    
    @Override
    public int hashCode() {
      return hash;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof Trace) {
        Trace t = (Trace) o;
        if (t.hash != hash) {
          return false;
        } else {
          return Arrays.equals(t.elements, elements);
        }
      } else {
        return false;
      }
    }
    
    @Override
    public int compareTo(Trace t) {
      return this.hash - t.hash;
    }
  }
  
  /**
   * <p>Class to represent a specific function call.  This is used so we can track how many times 
   * we see a given function.</p>
   * 
   * @author jent - Mike Jensen
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
      } else if (o instanceof Function) {
        Function m = (Function) o;
        return m.hashCode == hashCode && 
                 m.className.equals(className) && 
                 m.function.equals(function);
      } else {
        return false;
      }
    }
  }
}

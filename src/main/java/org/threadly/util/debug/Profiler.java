package org.threadly.util.debug;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.future.SettableListenableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

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
  protected static final short THREAD_PADDING_AMMOUNT = 10;
  protected static final short NUMBER_TARGET_LINE_LENGTH = 6;
  protected static final String THREAD_DELIMITER = "--------------------------------------------------";
  protected static final String FUNCTION_BY_NET_HEADER;
  protected static final String FUNCTION_BY_COUNT_HEADER;
  
  static {
    String prefix = "functions by ";
    String columns = "(total, top, name)";
    FUNCTION_BY_NET_HEADER = prefix + "top count: " + columns;
    FUNCTION_BY_COUNT_HEADER = prefix + "total count: " + columns;
  }
  
  protected final Map<String, Map<Trace, Trace>> threadTraces;
  protected final AtomicInteger collectedSamples;
  protected final File outputFile;
  protected final Object startStopLock;
  protected final AtomicReference<Thread> collectorThread;
  protected volatile int pollIntervalInMs;
  protected volatile Thread dumpingThread;
  private volatile ThreadIterator ti;
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@link #dump()} with a provided output stream to get the results to.  
   * 
   * This uses a default poll interval of 100 milliseconds.
   */
  public Profiler() {
    this(null, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance which will dump the results to the provided output file 
   * when {@link #stop()} is called.  
   * 
   * If the output file is {@code null}, this will behave the same as the empty constructor.  
   * 
   * This uses a default poll interval of 100 milliseconds.
   * 
   * @param outputFile file to dump results to on stop
   */
  public Profiler(File outputFile) {
    this(outputFile, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@link #dump()} with a provided output stream to get the results to.
   * 
   * @param pollIntervalInMs frequency to check running threads
   */
  public Profiler(int pollIntervalInMs) {
    this(null, pollIntervalInMs);
  }
  
  /**
   * Constructs a new profiler instance which will dump the results to the provided output file 
   * when {@link #stop()} is called.
   * 
   * If the output file is {@code null}, this will behave the same as the empty constructor.
   * 
   * @param outputFile file to dump results to on stop
   * @param pollIntervalInMs frequency to check running threads
   */
  public Profiler(File outputFile, int pollIntervalInMs) {
    setPollInterval(pollIntervalInMs);
    
    threadTraces = new ConcurrentHashMap<String, Map<Trace, Trace>>();
    collectedSamples = new AtomicInteger(0);
    this.outputFile = outputFile;
    startStopLock = new Object();
    collectorThread = new AtomicReference<Thread>(null);
    dumpingThread = null;
  }
  
  /**
   * Change how long the profiler waits before getting additional thread stacks.  This value must 
   * be greater than or equal to 0.
   * 
   * @param pollIntervalInMs time in milliseconds to wait between thread data dumps
   */
  public void setPollInterval(int pollIntervalInMs) {
    ArgumentVerifier.assertNotNegative(pollIntervalInMs, "pollIntervalInMs");
    
    this.pollIntervalInMs = pollIntervalInMs;
  }
  
  /**
   * Call to get the currently set profile interval.  This is the amount of time the profiler 
   * waits between collecting thread data.
   * 
   * @return returns the profile interval in milliseconds
   */
  public int getPollInterval() {
    return pollIntervalInMs;
  }
  
  /**
   * Call to get an estimate on how many times the profiler has collected a sample of the thread 
   * stacks.  This number may be lower than the actual sample quantity, but should never be 
   * higher.  It can be used to ensure a minimum level of accuracy from within the profiler.
   * 
   * @return the number of times since the start or last reset we have sampled the threads
   */
  public int getCollectedSampleQty() {
    return collectedSamples.get();
  }
  
  /**
   * Reset the current stored statistics.  The statistics will continue to grow in memory until 
   * the profiler is either stopped, or until this is called.
   */
  public void reset() {
    threadTraces.clear();
    collectedSamples.set(0);
  }
  
  /**
   * Call to check weather the profile is currently running/started.
   * 
   * @return {@code true} if there is a thread currently collecting statistics.
   */
  public boolean isRunning() {
    return collectorThread.get() != null;
  }
  
  /**
   * Starts the profiler running in a new thread.  
   * 
   * If this profiler had previously ran, and is now sitting in a stopped state again.  The 
   * statistics from the previous run will still be included in this run.  If you wish to clear 
   * out previous runs you must call {@link #reset()} first.
   */
  public void start() {
    start(null);
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
    synchronized (startStopLock) {
      if (collectorThread.get() == null) {
        final ProfilerRunner pr = new ProfilerRunner();
        if (executor == null) {
          // no executor, so we simply create our own thread
          Thread thread = new Thread(pr);
          
          collectorThread.set(thread);
          
          thread.setName("Profiler data collector");
          thread.setPriority(Thread.MAX_PRIORITY);
          thread.start();
        } else {
          final SettableListenableFuture<?> runningThreadFuture;
          runningThreadFuture = new SettableListenableFuture<Object>();
          
          executor.execute(new Runnable() {
            @Override
            public void run() {
              try {
                // if collector thread can't be set, then some other thread has taken over
                if (! collectorThread.compareAndSet(null, Thread.currentThread())) {
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
      Thread runningThread = collectorThread.get();
      if (runningThread != null) {
        runningThread.interrupt();
        collectorThread.set(null);
        if (ti != null) {
          ti = null;
        }
        
        if (outputFile != null) {
          try {
            OutputStream out = new FileOutputStream(outputFile);
            try {
              dump(out);
            } finally {
              out.close();
            }
          } catch (IOException e) {
            ExceptionUtils.handleException(e);
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
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    dump(new BufferedOutputStream(baos));
    
    return baos.toString();
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @param out OutputStream to write results to
   */
  public void dump(OutputStream out) {
    dump(new PrintStream(out, false));
  }
  
  /**
   * Output all the currently collected statistics to the provided output stream.
   * 
   * @param ps PrintStream to write results to
   */
  public void dump(PrintStream ps) {
    dumpingThread = Thread.currentThread();
    try {
      Map<Trace, Integer> globalTraces = new HashMap<Trace, Integer>();
      // create a local copy so the stats wont change while we are dumping them
      Map<String, Map<Trace, Trace>> threadTraces = new HashMap<String, Map<Trace, Trace>>(this.threadTraces);
      
      // log out individual thread traces
      Iterator<Entry<String, Map<Trace, Trace>>> it = threadTraces.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, Map<Trace, Trace>> entry = it.next();
        ps.println("Profile for thread: " + entry.getKey());
        dumpTraces(entry.getValue().keySet(), null, ps);
        
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
        
        ps.println(THREAD_DELIMITER);
        ps.println();
      }
        
      // log out global data
      if (globalTraces.size() > 1) {
        ps.println("Combined profile for all threads....");
        dumpTraces(globalTraces.keySet(), globalTraces, ps);
      }
      
      ps.flush();
    } finally {
      dumpingThread = null;
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
    
    out.println(" total count: " + format(total));
    out.println("native count: " + format(nativeCount));
    
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
    out.print(format(f.getCount()));
    out.print(format(f.getStackTopCount()));
    out.print(' ');
    out.print(f.className);
    out.print('.');
    out.println(f.function);
  }
  
  /**
   * Consistently formats an integer, adding spacing in front of it if necessary.
   * 
   * @param c Integer to use to represent within outputted string
   * @return Consistently sized string to represent the integer
   */
  private static String format(int c) {
    String s = Integer.toString(c);
    StringBuilder sb = new StringBuilder();
    
    while (sb.length() + s.length() < NUMBER_TARGET_LINE_LENGTH) {
      sb.append(' ');
    }
    
    sb.append(s);
    
    return sb.toString();
  }
  
  /**
   * A small call to get an iterator of threads that should be examined for this profiler cycle.  
   * 
   * This is a protected call, so it can be overridden to implement other profilers that want to 
   * control which threads are being profiled.
   * 
   * It is garunteed that this will be called in a single threaded manner.  In addition any 
   * previously returned Iterators will no longer be used by the time this one is called.
   * 
   * @return an {@link Iterator} of threads to examine and add data for our profile.
   */
  protected Iterator<Thread> getProfileThreadsIterator() {
    ThreadIterator result = ti;
    if (result == null) {
      result = new ThreadIterator();
      if (collectorThread.get() != null) {
        ti = result;
      }
    }
    result.refreshThreads();
    return result;
  }
  
  /**
   * <p>An iterator which will enumerate all the running threads within the VM.  You must call 
   * {@link #refreshThreads()} before it can be used.  It is expected that this iterator is NOT 
   * called in parallel.  When {@link #refreshThreads()} is called the iterator will reset to the 
   * starting position with current threads.</p>
   * 
   * <p>This class exists for two reasons...The first just to avoid another inner class.  The 
   * second is for a performance improvement for frequent profile polling.  If there is a large 
   * thread count we can avoid allocating a new Thread[] for each poll.  Instead we can just fill 
   * for the current threads, then null out any former threads that existed.  This can provide 
   * much more accurate polling</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.4.0
   */
  protected static class ThreadIterator implements Iterator<Thread> {
    protected Thread[] threads = new Thread[0];
    protected int enumerateCount = 0;
    protected int currentIndex = 0;
    
    public void refreshThreads() {
      int minThreadArraySize = Thread.activeCount() + THREAD_PADDING_AMMOUNT;
      int previousEnumerateCount;
      if (threads.length < minThreadArraySize) {
        threads = new Thread[minThreadArraySize];
        previousEnumerateCount = 0;
      } else {
        previousEnumerateCount = enumerateCount;
      }
      enumerateCount = Thread.enumerate(threads);
      // null out any threads no longer tracked (to avoid memory leak)
      if (enumerateCount < previousEnumerateCount) {
        Arrays.fill(threads, enumerateCount, previousEnumerateCount, null);
      }
      currentIndex = 0;
    }
    
    @Override
    public boolean hasNext() {
      return currentIndex < enumerateCount;
    }

    @Override
    public Thread next() {
      if (hasNext()) {
        return threads[currentIndex++];
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  /**
   * <p>Class which runs, collecting statistics for the profiler to later analyze.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  private class ProfilerRunner implements Runnable {
    @Override
    public void run() {
      Thread runningThread = Thread.currentThread();
      while (collectorThread.get() == runningThread) {
        boolean storedSample = false;
        Iterator<Thread> it = getProfileThreadsIterator();
        while (it.hasNext()) {
          Thread currentThread = it.next();
          
          // we skip the Profiler threads (collector thread, and dumping thread if one exists)
          if (currentThread != runningThread && 
              currentThread != dumpingThread) {
            StackTraceElement[] threadStack = currentThread.getStackTrace();
            if (threadStack.length > 0) {
              storedSample = true;
              String threadIdentifier = getThreadIdentifier(currentThread);
              Trace t = new Trace(threadStack);
              
              Map<Trace, Trace> existingTraces = threadTraces.get(threadIdentifier);
              if (existingTraces == null) {
                existingTraces = new ConcurrentHashMap<Trace, Trace>();
                threadTraces.put(threadIdentifier, existingTraces);
  
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
          collectedSamples.incrementAndGet();
        }
        try {
          Thread.sleep(pollIntervalInMs);
        } catch (InterruptedException e) {
          collectorThread.compareAndSet(runningThread, null);
          Thread.currentThread().interrupt(); // reset status
          return;
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
        } else if (t.elements.length != elements.length) {
          return false;
        } else {
          for (int i = 0; i < elements.length; i++) {
            if (! t.elements[i].equals(elements[i])) {
              return false;
            }
          }
        }
        
        return true;
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

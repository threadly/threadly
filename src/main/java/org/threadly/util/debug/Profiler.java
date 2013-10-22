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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.util.ExceptionUtils;

/**
 * Tool for profiling a running java application to get an idea 
 * of where the slow points (either because of lock contention, or 
 * because of high computational demand).
 * 
 * This tool definitely incurs some load within the system, so it 
 * should only be used while debugging, and not as general use.  In 
 * addition if it is left running without being reset, it will 
 * continue to consume more and more memory.
 * 
 * @author jent - Mike Jensen
 */
public class Profiler implements Runnable {
  protected static final short DEFAULT_POLL_INTERVAL_IN_MILLIS = 100;
  protected static final short THREAD_PADDING_AMMOUNT = 10;
  protected static final short NUMBER_TARGET_LINE_LENGTH = 6;
  protected static final String COLLECTOR_THREAD_NAME = "Profiler data collector";
  protected static final String THREAD_DELIMITER = "--------------------------------------------------\n";
  protected static final String FUNCTION_BY_NET_HEADER = "\nfunctions by top count: (total, top, name)\n";
  protected static final String FUNCTION_BY_COUNT_HEADER = "\nfunction by total count: (total, top, name)\n";
  
  private final Map<String, Map<Trace, Trace>> threadTraces;
  private final File outputFile;
  private final int pollIntervalInMs;
  private final Object startStopLock;
  private final AtomicReference<Thread> collectorThread;
  private volatile Thread dumpingThread;
  
  /**
   * Constructs a new profiler instance.  The only way 
   * to get results from this instance is to call "dump" 
   * with a provided output stream to get the results to.
   */
  public Profiler() {
    this(null, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance which will dump the 
   * results to the provided output file when "stop" is called.
   * 
   * If the output file is null, this will behave the same as the 
   * empty constructor.
   * 
   * @param outputFile file to dump results to on stop
   */
  public Profiler(File outputFile) {
    this(outputFile, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance.  The only way 
   * to get results from this instance is to call "dump" 
   * with a provided output stream to get the results to.
   * 
   * @param pollIntervalInMs frequency to check running threads
   */
  public Profiler(int pollIntervalInMs) {
    this(null, pollIntervalInMs);
  }
  
  /**
   * Constructs a new profiler instance which will dump the 
   * results to the provided output file when "stop" is called.
   * 
   * If the output file is null, this will behave the same as the 
   * empty constructor.
   * 
   * @param outputFile file to dump results to on stop
   * @param pollIntervalInMs frequency to check running threads
   */
  public Profiler(File outputFile, int pollIntervalInMs) {
    threadTraces = new ConcurrentHashMap<String, Map<Trace, Trace>>();
    this.outputFile = outputFile;
    this.pollIntervalInMs = pollIntervalInMs;
    startStopLock = new Object();
    collectorThread = new AtomicReference<Thread>(null);
    dumpingThread = null;
  }
  
  /**
   * Reset the current stored statistics.  The statistics will 
   * continue to grow in memory until the profiler is either stopped, 
   * or until this is called.
   */
  public void reset() {
    threadTraces.clear();
  }
  
  /**
   * Starts the profiler running in a new thread.
   * 
   * If this profiler had the life cycle of: 
   * start -> stop -> and now your calling start again
   * 
   * The stats from the previous run will still be included 
   * in this run.  If you wish to clear out previous runs 
   * you must call {#link reset()} first.
   */
  public synchronized void start() {
    synchronized (startStopLock) {
      if (collectorThread.get() == null) {
        Thread thread = new Thread(this);
        
        collectorThread.set(thread);
        
        thread.setName(COLLECTOR_THREAD_NAME);
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
      }
    }
  }
  
  /**
   * Stops the profiler from collecting more statistics.  If 
   * a file was provided at construction, the results will be 
   * written to that file.  It is possible to request the 
   * results using the {#link dump()} call after it has stopped.
   */
  public void stop() {
    synchronized (startStopLock) {
      Thread runningThread = collectorThread.get();
      if (runningThread != null) {
        runningThread.interrupt();
        collectorThread.set(null);
        
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
  
  private static String getThreadIdentifier(Thread t) {
    return t.toString() + ';' + Long.toHexString(t.getId());
  }
  
  @Override
  public void run() {
    Thread runningThread = Thread.currentThread();
    while (collectorThread.get() == runningThread) {
      int count = Thread.activeCount();
      // we add a little to make sure we get every thread
      Thread[] threads = new Thread[count + THREAD_PADDING_AMMOUNT];
      count = Thread.enumerate(threads);
      for (int i = 0; i < count; i++) {
        Thread currentThread = threads[i];
        // we skip the Profiler threads (collector thread, and dumping thread if one exists)
        if (currentThread != runningThread && 
            currentThread != dumpingThread) {
          StackTraceElement[] threadStack = currentThread.getStackTrace();
          if (threadStack.length > 0) {
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
                existingTrace.threadCount++;
              }
            }
          }
        }
      }

      try {
        Thread.sleep(pollIntervalInMs);
      } catch (InterruptedException e) {
        collectorThread.compareAndSet(runningThread, null);
        return;
      }
    }
  }
  
  /**
   * Output all the currently collected statistics to the provided output 
   * stream.
   * 
   * @return The dumped results as a single String
   */
  public String dump() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    dump(new BufferedOutputStream(baos));
    
    return baos.toString();
  }
  
  /**
   * Output all the currently collected statistics to the provided output 
   * stream.
   * 
   * @param out OutputStream to write results to
   */
  public void dump(OutputStream out) {
    dump(new PrintStream(out, false));
  }
  
  /**
   * Output all the currently collected statistics to the provided output 
   * stream.
   * 
   * @param ps PrintStream to write results to
   */
  public void dump(PrintStream ps) {
    dumpingThread = Thread.currentThread();
    try {
      Map<Trace, Trace> globalTraces = new HashMap<Trace, Trace>();
      // create a local copy so the stats wont change while we are dumping them
      Map<String, Map<Trace, Trace>> threadTraces = new HashMap<String, Map<Trace, Trace>>(this.threadTraces);
      
      // log out individual thread traces
      Iterator<Entry<String, Map<Trace, Trace>>> it = threadTraces.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, Map<Trace, Trace>> entry = it.next();
        ps.println("Profile for thread: " + entry.getKey());
        dumpTraces(entry.getValue().keySet(), false, ps);
        
        // add in this threads trace data to the global trace map
        Iterator<Trace> traceIt = entry.getValue().keySet().iterator();
        while (traceIt.hasNext()) {
          Trace currTrace = traceIt.next();
          Trace storedTrace = globalTraces.get(currTrace);
          if (storedTrace == null) {
            // make sure this is reset in case we dump multiple times
            currTrace.globalCount = currTrace.threadCount;
            globalTraces.put(currTrace, currTrace);
          } else {
            storedTrace.globalCount += currTrace.threadCount;
          }
        }
        
        ps.println(THREAD_DELIMITER);
      }
        
      // log out global data
      ps.println("Combined profile for all threads....");
      dumpTraces(globalTraces.keySet(), true, ps);
      
      ps.flush();
    } finally {
      dumpingThread = null;
    }
  }
  
  private static void dumpTraces(Set<Trace> traces, 
                                 boolean globalCount, 
                                 PrintStream out) {
    Map<Function, Function> methods = new HashMap<Function, Function>();
    Trace[] traceArray = traces.toArray(new Trace[traces.size()]);
    int total = 0;
    int nativeCount = 0;
    
    for (Trace t: traceArray) {
      if (globalCount) {
        total += t.globalCount;
      } else {
        total += t.threadCount;
      }
      
      if (t.elements.length > 0 && 
          t.elements[0].isNativeMethod()) {
        if (globalCount) {
          nativeCount += t.globalCount;
        } else {
          nativeCount += t.threadCount;
        }
      }
      
      for (int i = 0; i < t.elements.length; ++i) {
        Function n = new Function(t.elements[i].getClassName(),
                                  t.elements[i].getMethodName());
        Function f = methods.get(n);
        if (f == null) {
          methods.put(n, n);
          f = n;
        }
        if (globalCount) {
          f.count += t.globalCount;
        } else {
          f.count += t.threadCount;
        }
        if (i > 0) {
          if (globalCount) {
            f.childCount += t.globalCount;
          } else {
            f.childCount += t.threadCount;
          }
        }
      }
    }
    
    Function[] methodArray = methods.keySet().toArray(new Function[methods.size()]);
    
    out.println(" total count: " + format(total));
    out.println("native count: " + format(nativeCount));
    
    out.println(FUNCTION_BY_NET_HEADER);
    
    Arrays.sort(methodArray, new Comparator<Function>() {
      @Override
      public int compare(Function a, Function b) {
        return (b.count - b.childCount) - (a.count - a.childCount);
      }
    });
    
    for (int i = 0; i < methodArray.length; i++) {
      dumpFunction(methodArray[i], out);
    }
    
    out.println(FUNCTION_BY_COUNT_HEADER);
    
    Arrays.sort(methodArray, new Comparator<Function>() {
      @Override
      public int compare(Function a, Function b) {
        return b.count - a.count;
      }
    });
    
    for (int i = 0; i < methodArray.length; i++) {
      dumpFunction(methodArray[i], out);
    }
    
    out.println("\ntraces by count:\n");
    
    if (globalCount) {
      Arrays.sort(traceArray, new Comparator<Trace>() {
        @Override
        public int compare(Trace a, Trace b) {
          return b.globalCount - a.globalCount;
        }
      });
    } else {
      Arrays.sort(traceArray, new Comparator<Trace>() {
        @Override
        public int compare(Trace a, Trace b) {
          return b.threadCount - a.threadCount;
        }
      });
    }
    
    for (int i = 0; i < traceArray.length; i++) {
      Trace t = traceArray[i];
      int count;
      if (globalCount) {
        count = t.globalCount;
      } else {
        count = t.threadCount;
      }
      out.println(count + " time(s):");
      
      for (int j = 0; i < t.elements.length; j++) {
        out.println("  at " + t.elements[j].toString());
      }
      
      out.println();
    }
  }
  
  private static void dumpFunction(Function m, PrintStream out) {
    out.print(format(m.count));
    out.print(format(m.count - m.childCount));
    out.print(' ');
    out.print(m.className);
    out.print('.');
    out.println(m.function);
  }
  
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
   * Class which represents a stack trace.  The is used so 
   * we can track how many times a given stack is seen.
   * 
   * This stack trace reference will be specific to a single thread.
   * 
   * @author jent - Mike Jensen
   */
  private static class Trace implements Comparable<Trace> {
    private final StackTraceElement[] elements;
    private final int hash;
    private int threadCount = 1;  // is increased as seen for a specific thread
    private int globalCount = 1;  // is only set when dumping the statistics
    
    public Trace(StackTraceElement[] elements) {
      this.elements = elements;
      
      int h = 0;
      for (StackTraceElement e: elements) {
        h ^= e.hashCode();
      }
      hash = h;
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
   * Class to represent a specific function call.  
   * This is used so we can track how many times we 
   * see a given function.
   * 
   * @author jent - Mike Jensen
   */
  private static class Function {
    private final String className;
    private final String function;
    private final int hashCode;
    private int count;
    private int childCount;
    
    public Function(String className, String function) {
      this.className = className;
      this.function = function;
      this.hashCode = className.hashCode() ^ function.hashCode();
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

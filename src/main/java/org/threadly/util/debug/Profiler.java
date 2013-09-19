package org.threadly.util.debug;

import java.io.File;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Arrays;

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
  private static final int DEFAULT_POLL_INTERVAL_IN_MILLIS = 100;
  
  private final Map<String, Map<Trace, Trace>> threadTraces;
  private final File outputFile;
  private final int pollIntervalInMs;
  private Thread thread = null;
  
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
    threadTraces = new HashMap<String, Map<Trace, Trace>>();
    this.outputFile = outputFile;
    this.pollIntervalInMs = pollIntervalInMs;
  }
  
  /**
   * Reset the current stored staticistics.  The statistics will 
   * continue to grow in memory until the profiler is either stopped, 
   * or until this is called.
   */
  public void reset() {
    synchronized (this) { // TODO - make more concurrent
      threadTraces.clear();
    }
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
    synchronized (this) {
      if (thread == null) {
        thread = new Thread(this);
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
    synchronized (this) {
      if (thread != null) {
        thread = null;
        
        if (outputFile != null) {
          try {
            OutputStream out = new FileOutputStream(outputFile);
            try {
              dump(out);
            } finally {
              out.close();
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
  
  private static String getThreadIdentifier(Thread t) {
    return t.toString() + ";" + Long.toHexString(t.getId());
  }
  
  @Override
  public void run() {
    synchronized (this) { // TODO - make more concurrent
      while (thread != null) {
        try {
          wait(pollIntervalInMs);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        
        int count = Thread.activeCount();
        Thread[] threads = new Thread[count + 10];  // we add a little to make sure we get every thread
        count = Thread.enumerate(threads);
        for (int i = 0; i < count; i++) {
          if (threads[i] != thread) {
            String threadIdentifier = getThreadIdentifier(threads[i]);
            Trace t = new Trace(threads[i].getStackTrace());
            
            Map<Trace, Trace> existingTraces = threadTraces.get(threadIdentifier);
            if (existingTraces == null) {
              existingTraces = new HashMap<Trace, Trace>();
              threadTraces.put(threadIdentifier, existingTraces);
            }
            
            Trace existingTrace = existingTraces.get(t);
            if (existingTrace == null) {
              existingTraces.put(t, t);
            } else {
              existingTrace.count++;
            }
          }
        }
      }
    }
  }
  
  /**
   * Output all the currently collected statistics to the provided output 
   * stream.
   * 
   * @param out OutputStream to write results to
   * @throws IOException exception possible from writing to stream
   */
  public void dump(OutputStream out) throws IOException {
    PrintStream ps = new PrintStream(out);
    Map<Trace, Trace> globalTraces = new HashMap<Trace, Trace>();
    
    synchronized (this) { // TODO - make more concurrent
      // log out individual thread traces
      Iterator<Entry<String, Map<Trace, Trace>>> it = threadTraces.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, Map<Trace, Trace>> entry = it.next();
        ps.println("Profile for thread: " + entry.getKey());
        dump(entry.getValue().keySet(), ps);
        
        // add in this threads trace data to the global trace map
        Iterator<Trace> traceIt = entry.getValue().keySet().iterator();
        while (traceIt.hasNext()) {
          Trace currTrace = traceIt.next();
          Trace storedTrace = globalTraces.get(currTrace);
          if (storedTrace == null) {
            globalTraces.put(currTrace, currTrace);
          } else {
            storedTrace.count += currTrace.count;
          }
        }
        
        ps.println("\n-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n");
      }
      
      // log out global data
      ps.println("Combined profile for all threads....");
      dump(globalTraces.keySet(), ps);
    }
    
    ps.flush();
  }
  
  private static void dump(Set<Trace> traces, 
                           PrintStream out) {
    Map<Method, Method> methods = new HashMap<Method, Method>();
    Trace[] traceArray = new Trace[traces.size()];
    int total = 0;
    int nativeCount = 0;
    
    int index = 0;
    for (Trace t: traces) {
      traceArray[index++] = t;
      
      total += t.count;
      
      if (t.elements.length > 0 && 
          t.elements[0].isNativeMethod()) {
        nativeCount += t.count;
      }
      
      for (int j = 0; j < t.elements.length; ++j) {
        Method n = new Method(t.elements[j].getClassName(),
                              t.elements[j].getMethodName());
        Method m = methods.get(n);
        if (m == null) {
          methods.put(n, n);
          m = n;
        }
        m.count += t.count;
        if (j > 0) {
          m.childCount += t.count;
        }
      }
    }
    
    Method[] methodArray = methods.keySet().toArray(new Method[methods.size()]);
    
    out.println(" total count: " + format(total));
    out.println("native count: " + format(nativeCount));
    
    out.println("\nmethods by count: (total, net, name)\n");
    
    Arrays.sort(methodArray, new Comparator<Method>() {
      public int compare(Method a, Method b) {
        return b.count - a.count;
      }
    });
    
    for (int i = 0; i < methodArray.length; i++) {
      dump(methodArray[i], out);
    }
    
    out.println("\nmethods by net count: (total, net, name)\n");
    
    Arrays.sort(methodArray, new Comparator<Method>() {
      public int compare(Method a, Method b) {
        return (b.count - b.childCount) - (a.count - a.childCount);
      }
    });
    
    for (int i = 0; i < methodArray.length; i++) {
      dump(methodArray[i], out);
    }
    
    out.println();
    out.println("traces by count:");
    out.println();
    
    Arrays.sort(traceArray, new Comparator<Trace>() {
      public int compare(Trace a, Trace b) {
        return b.count - a.count;
      }
    });
    
    for (int i = 0; i < traceArray.length; i++) {
      dump(traceArray[i], out);
      out.println();
    }
  }
  
  private static void dump(Trace t, PrintStream out) {
    out.println(t.count + " time(s):");
    for (int i = 0; i < t.elements.length; ++i) {
      out.println("  at " + t.elements[i].toString());
    }
  }
  
  private static void dump(Method m, PrintStream out) {
    out.print(format(m.count));
    out.print(format(m.count - m.childCount));
    out.print(" ");
    out.print(m.classStr);
    out.print(".");
    out.println(m.method);
  }
  
  private static String format(int c) {
    String s = Integer.toString(c);
    StringBuilder sb = new StringBuilder();
    
    while (sb.length() + s.length() < 6) {
      sb.append(' ');
    }
    
    sb.append(s);
    
    return sb.toString();
  }
  
  private static class Trace implements Comparable<Trace> {
    private final StackTraceElement[] elements;
    private final int hash;
    private int count = 1;
    
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
  
  private static class Method {
    private final String classStr;
    private final String method;
    private final int hashCode;
    private int count;
    private int childCount;
    
    public Method(String classStr, String method) {
      this.classStr = classStr;
      this.method = method;
      this.hashCode = classStr.hashCode() ^ method.hashCode();
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof Method) {
        Method m = (Method) o;
        return m.hashCode == hashCode && 
                 m.classStr.equals(classStr) && 
                 m.method.equals(method);
      } else {
        return false;
      }
    }
  }
}

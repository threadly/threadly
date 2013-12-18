package org.threadly.util.debug;

import java.io.File;
import java.util.Iterator;

import org.threadly.concurrent.collections.ConcurrentArrayList;

/**
 * <p>This class functions very similar to the {@link Profiler}.  The difference 
 * between the two is while the {@link Profiler} profiles all running threads on 
 * the VM.  This implementation only profiles threads which you explicitly add to 
 * be profiled.</p>
 * 
 * @author jent - Mike Jensen
 */
public class ControlledThreadProfiler extends Profiler {
  private static final int TRACKED_THREAD_BUFFER = 10;  // used to make adding/removing tracked threads more efficient
  
  protected final ConcurrentArrayList<Thread> profiledThreads;
  
  /**
   * Constructs a new profiler instance.  The only way 
   * to get results from this instance is to call "dump" 
   * with a provided output stream to get the results to.
   */
  public ControlledThreadProfiler() {
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
  public ControlledThreadProfiler(File outputFile) {
    this(outputFile, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance.  The only way 
   * to get results from this instance is to call "dump" 
   * with a provided output stream to get the results to.
   * 
   * @param pollIntervalInMs frequency to check running threads
   */
  public ControlledThreadProfiler(int pollIntervalInMs) {
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
  public ControlledThreadProfiler(File outputFile, int pollIntervalInMs) {
    super(outputFile, pollIntervalInMs);
    
    profiledThreads = new ConcurrentArrayList<Thread>(0, TRACKED_THREAD_BUFFER);
  }
  
  /**
   * Adds a thread to be checked by the running profiler.
   * 
   * If the thread is already included, or if the thread is 
   * null, this is a no-op.
   * 
   * @param t Thread to add to the list of tracked threads
   */
  public void addProfiledThread(Thread t) {
    if (t == null) {
      return; // don't add
    }
    
    synchronized (profiledThreads.getModificationLock()) {
      if (! profiledThreads.contains(t)) {
        profiledThreads.add(t);
      }
    }
  }
  
  /**
   * Removed a thread from the set of tracked threads.
   * 
   * @param t Thread to remove from tracked set
   * @return true if the thread was found and removed.
   */
  public boolean removedProfiledThread(Thread t) {
    return profiledThreads.remove(t);
  }
  
  @Override
  protected Iterator<Thread> getProfileThreadsIterator() {
    return profiledThreads.iterator();
  }
}

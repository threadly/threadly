package org.threadly.util.debug;

import java.io.File;
import java.util.Iterator;

import org.threadly.concurrent.collections.ConcurrentArrayList;

/**
 * <p>This class functions very similar to the {@link Profiler}.  The difference between the two 
 * is while the {@link Profiler} profiles all running threads on the VM.  This implementation only 
 * profiles threads which you explicitly add to be profiled.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ControlledThreadProfiler extends Profiler {
  private static final int TRACKED_THREAD_BUFFER = 10;  // used to make adding/removing tracked threads more efficient
  
  protected final ControlledThreadProfileStorage controledThreadStore;
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@code #dump()} with a provided output stream to get the results to.
   */
  public ControlledThreadProfiler() {
    this(null, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance which will dump the results to the provided output file 
   * when {@code #stop()} is called.
   * 
   * If the output file is null, this will behave the same as the empty constructor.
   * 
   * @param outputFile file to dump results to on stop
   */
  public ControlledThreadProfiler(File outputFile) {
    this(outputFile, DEFAULT_POLL_INTERVAL_IN_MILLIS);
  }
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to call 
   * {@code #dump()} with a provided output stream to get the results to.
   * 
   * @param pollIntervalInMs frequency to check running threads
   */
  public ControlledThreadProfiler(int pollIntervalInMs) {
    this(null, pollIntervalInMs);
  }
  
  /**
   * Constructs a new profiler instance which will dump the results to the provided output file 
   * when {@code #stop()} is called.
   * 
   * If the output file is {@code null}, this will behave the same as the empty constructor.
   * 
   * @param outputFile file to dump results to on stop
   * @param pollIntervalInMs frequency to check running threads
   */
  public ControlledThreadProfiler(File outputFile, int pollIntervalInMs) {
    super(outputFile, new ControlledThreadProfileStorage(pollIntervalInMs));
    
    controledThreadStore = (ControlledThreadProfileStorage)super.pStore;
  }
  
  /**
   * Adds a thread to be checked by the running profiler.  
   * 
   * If the thread is already included, or if the thread is {@code null}, this is a no-op.
   * 
   * @param t Thread to add to the list of tracked threads
   */
  public void addProfiledThread(Thread t) {
    if (t == null) {
      return; // don't add
    }
    
    synchronized (controledThreadStore.profiledThreads.getModificationLock()) {
      if (! controledThreadStore.profiledThreads.contains(t)) {
        controledThreadStore.profiledThreads.add(t);
      }
    }
  }
  
  /**
   * Removed a thread from the set of tracked threads.  It is good practice to remove a thread 
   * from the profiler if it is no longer alive.  This profiler makes NO attempt to automatically 
   * remove dead threads.
   * 
   * @param t Thread to remove from tracked set
   * @return {@code true} if the thread was found and removed.
   */
  public boolean removedProfiledThread(Thread t) {
    return controledThreadStore.profiledThreads.remove(t);
  }
  
  /**
   * Call to check how many threads are currently being checked by the profiler.  Keep in mind 
   * that threads that are not alive may be skipped by the profiler, but will be included in this 
   * count.
   * 
   * @return count of tracked threads.
   */
  public int getProfiledThreadCount() {
    return controledThreadStore.profiledThreads.size();
  }
  
  /**
   * <p>Extending class of {@link ProfileStorage} this overrides 
   * {@link #getProfileThreadsIterator()}.  It controls it so that not all VM threads are returned 
   * in the iterator, and instead it only iterates over the threads which are stored 
   * internally.</p>
   * 
   * @author jent
   * @since 3.5.0
   */
  protected static class ControlledThreadProfileStorage extends ProfileStorage {
    protected final ConcurrentArrayList<Thread> profiledThreads;

    public ControlledThreadProfileStorage(int pollIntervalInMs) {
      super(pollIntervalInMs);
      
      profiledThreads = new ConcurrentArrayList<Thread>(0, TRACKED_THREAD_BUFFER);
    }
    
    @Override
    protected Iterator<Thread> getProfileThreadsIterator() {
      return profiledThreads.iterator();
    }
  }
}

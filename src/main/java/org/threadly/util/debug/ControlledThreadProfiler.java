package org.threadly.util.debug;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;

/**
 * This class functions very similar to the {@link Profiler}.  The difference between the two is 
 * while the {@link Profiler} profiles all running threads on the VM.  This implementation only 
 * profiles threads which you explicitly add to be profiled.
 * <p>
 * It can be useful to use a {@link org.threadly.concurrent.ConfigurableThreadFactory} where 
 * {@link #addProfiledThread(Thread)} is provided as the {@link java.util.function.Consumer} on 
 * thread creation.
 * 
 * @since 1.0.0
 */
public class ControlledThreadProfiler extends Profiler {
  private static final short TRACKED_THREAD_BUFFER = 10;  // used to make adding/removing tracked threads more efficient
  
  protected final ControlledThreadProfileStorage controledThreadStore;
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to 
   * call {@code #dump()} with a provided output stream to get the results to.
   */
  public ControlledThreadProfiler() {
    this(DEFAULT_POLL_INTERVAL_IN_MILLIS, null);
  }
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to call 
   * {@code #dump()} with a provided output stream to get the results to.
   * 
   * @param pollIntervalInMs frequency to check running threads
   */
  public ControlledThreadProfiler(int pollIntervalInMs) {
    this(pollIntervalInMs, null);
  }
  
  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to call 
   * {@code #dump()} with a provided output stream to get the results to.
   * <p>
   * This constructor allows you to change the behavior of the {@link ListenableFuture} result when 
   * {@link #start(long)} or {@link #start(Executor, long)} is used.  Generally this will provide 
   * the complete result of {@link #dump()}.  This can be replaced with calling 
   * {@link #dump(OutputStream, boolean, int)} with parameters to reduce the output, or even 
   * {@code null} so long as the consumers of the future can handle a null result.
   * 
   * @param pollIntervalInMs frequency to check running threads
   * @param startFutureResultSupplier Supplier to be used for providing future results
   */
  public ControlledThreadProfiler(int pollIntervalInMs, 
                                  Function<? super Profiler, String> startFutureResultSupplier) {
    super(new ControlledThreadProfileStorage(pollIntervalInMs), startFutureResultSupplier);
    
    controledThreadStore = (ControlledThreadProfileStorage)super.pStore;
  }
  
  /**
   * Adds a thread to be checked by the running profiler.  
   * <p>
   * If the thread is already included, or if the thread is {@code null}, this is a no-op.
   * 
   * @param t Thread to add to the list of tracked threads
   */
  public void addProfiledThread(Thread t) {
    if (t == null) {
      return; // don't add
    }
    
    synchronized (controledThreadStore.profiledThreads.getModificationLock()) {
      SelectedThreadSample threadSample = new SelectedThreadSample(t);
      if (! controledThreadStore.profiledThreads.contains(threadSample)) {
        controledThreadStore.profiledThreads.add(threadSample);
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
  public boolean removeProfiledThread(Thread t) {
    return controledThreadStore.profiledThreads.remove(new SelectedThreadSample(t));
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
   * Extending class of {@link ProfileStorage} this overrides
   * {@link #getProfileThreadsIterator()}.  It controls it so that not all VM threads are returned
   * in the iterator, and instead it only iterates over the threads which are stored internally.
   *
   * @since 3.5.0
   */
  protected static class ControlledThreadProfileStorage extends ProfileStorage {
    protected final ConcurrentArrayList<SelectedThreadSample> profiledThreads;

    public ControlledThreadProfileStorage(int pollIntervalInMs) {
      super(pollIntervalInMs);

      profiledThreads = new ConcurrentArrayList<>(0, TRACKED_THREAD_BUFFER);
    }

    @Override
    protected Iterator<? extends ThreadSample> getProfileThreadsIterator() {
      // Must use getAllStackTraces to get consistent traces (matching CommonStacktraces format)
      Map<Thread, StackTraceElement[]> allTraces = Thread.getAllStackTraces();
      List<ThreadSample> result = new ArrayList<>(profiledThreads.size());
      for (SelectedThreadSample sample : profiledThreads) {
        Thread t = sample.getThread();
        StackTraceElement[] trace = allTraces.get(t);
        if (trace != null) {
          result.add(new CachedThreadSample(t, trace));
        }
      }
      return result.iterator();
    }
  }

  /**
   * A wrapper for a thread to implement {@link ThreadSample}.  This wrapper allows us to just use
   * a simple collection for storing threads (and thus to iterate over), and then lazily
   * generate the stack trace when {@link ThreadSample#getStackTrace()} is invoked.
   *
   * @since 3.8.0
   */
  protected static class SelectedThreadSample implements ThreadSample {
    private final Thread t;

    protected SelectedThreadSample(Thread t) {
      this.t = t;
    }

    @Override
    public Thread getThread() {
      return t;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
      return t.getStackTrace();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof ThreadSample) {
        return ((ThreadSample)o).getThread() == t;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return t.hashCode();
    }
  }
}

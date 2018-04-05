package org.threadly.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandler;

/**
 * A thread factory which keeps a {@link WeakReference} to each thread.  These threads (if still 
 * referenced by the VM) can then be retrieved by using {@link #getThreads(boolean)}.
 * 
 * @since 5.15
 */
public class ThreadReferencingThreadFactory extends ConfigurableThreadFactory {
  private static final int REFERENCE_QUEUE_CHECK_INTERVAL_MILLIS = 10_000;
  
  private final Collection<WeakReference<Thread>> threads = new ConcurrentLinkedQueue<>();
  private volatile long lastCleanupTime = Clock.lastKnownForwardProgressingMillis();
  
  /**
   * Constructs a new {@link ThreadReferencingThreadFactory} with the default parameters.  Threads 
   * produced from this should behave exactly like Executors.defaultThreadFactory(), except the 
   * pool number provided in the thread name will be respective to the ones created from other 
   * {@link ThreadReferencingThreadFactory} instances.
   */
  public ThreadReferencingThreadFactory() {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, null, null);
  }
  
  /**
   * Constructs a new {@link ThreadReferencingThreadFactory} specifying the prefix for the name of 
   * newly created threads.  
   * <p>
   * If specified with {@code true} for {@code appendPoolIdToPrefix} it will append a unique 
   * "pool" id to the prefix, giving it the format of 
   * {@code threadNamePrefix + UNIQUE_POOL_ID + "-thread-"}.  If {@code appendPoolIdToPrefix} is 
   * specified as {@code false}, only a unique thread id will be appended to the prefix.  In 
   * either case, the produced threads name will be appended with a unique thread id for the 
   * factory instance.
   * 
   * @param threadNamePrefix prefix for all threads created
   * @param appendPoolIdToPrefix {@code true} to append a unique pool id to the thread prefix
   */
  public ThreadReferencingThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix) {
    this(threadNamePrefix, appendPoolIdToPrefix, 
         DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, null, null);
  }
  
  /**
   * Constructs a new {@link ThreadReferencingThreadFactory} specifying the behavior for if threads 
   * should be daemon or not.
   * 
   * @param useDaemonThreads {@code true} if produced threads should be daemon threads
   */
  public ThreadReferencingThreadFactory(boolean useDaemonThreads) {
    this(null, true, useDaemonThreads, Thread.NORM_PRIORITY, null, null);
  }
  
  /**
   * Constructs a new {@link ThreadReferencingThreadFactory} specifying the priority for produced 
   * threads.  
   * <p>
   * If the priority is below or above the max available thread priority, this will be adjusted to 
   * the limit of the system.
   * 
   * @param threadPriority Priority for newly created threads
   */
  public ThreadReferencingThreadFactory(int threadPriority) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, threadPriority, null, null);
  }
  
  /**
   * Constructs a new {@link ThreadReferencingThreadFactory} specifying an 
   * {@link UncaughtExceptionHandler} that will be provided to all newly created threads.
   * 
   * @param defaultUncaughtExceptionHandler {@link UncaughtExceptionHandler} to provide to newly created threads
   */
  public ThreadReferencingThreadFactory(UncaughtExceptionHandler defaultUncaughtExceptionHandler) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, 
         Thread.NORM_PRIORITY, defaultUncaughtExceptionHandler, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying an {@link ExceptionHandler} 
   * that will be provided to all newly created threads.
   * 
   * @param defaultThreadlyExceptionHandler {@link ExceptionHandler} to provide to newly created threads
   */
  public ThreadReferencingThreadFactory(ExceptionHandler defaultThreadlyExceptionHandler) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, 
         Thread.NORM_PRIORITY, null, defaultThreadlyExceptionHandler);
  }
  
  /**
   * Constructs a new {@link ThreadReferencingThreadFactory} allowing you to provide specific values 
   * for everything which this class allows to be configured.  You must use this constructor if 
   * you need to adjust two or more values.  
   * <p>
   * If specified with {@code true} for {@code appendPoolIdToPrefix} it will append a unique 
   * "pool" id to the prefix, giving it the format of 
   * {@code threadNamePrefix + UNIQUE_POOL_ID + "-thread-"}.  If {@code appendPoolIdToPrefix} is 
   * specified as {@code false}, only a unique thread id will be appended to the prefix.  In 
   * either case, the produced threads name will be appended with a unique thread id for the 
   * factory instance.
   * <p>
   * If the priority is below or above the max available thread priority, this will be adjusted to 
   * the limit of the system.
   * 
   * @param threadNamePrefix prefix for all threads created, {@code null} to match default
   * @param appendPoolIdToPrefix {@code true} to append a unique pool id to the thread prefix, 
   *                             {@code true} to match default
   * @param useDaemonThreads true if produced threads should be daemon threads, false to match default
   * @param threadPriority Priority for newly created threads, {@code Thread.NORM_PRIORITY} to match default
   * @param uncaughtExceptionHandler UncaughtExceptionHandler to provide to newly created threads, 
   *                                 {@code null} to match default
   * @param defaultThreadlyExceptionHandler {@link ExceptionHandler} to provide to newly created threads
   */
  public ThreadReferencingThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix, 
                                        boolean useDaemonThreads, int threadPriority, 
                                        UncaughtExceptionHandler uncaughtExceptionHandler, 
                                        ExceptionHandler defaultThreadlyExceptionHandler) {
    super(threadNamePrefix, appendPoolIdToPrefix, useDaemonThreads, threadPriority, 
          uncaughtExceptionHandler, defaultThreadlyExceptionHandler);
  }
  
  /**
   * Get a list of currently known threads.  This provides the ability to check if the thread is 
   * still alive before it is added, but threads may die / stop while the result is building so 
   * inactive threads may still be returned.
   * 
   * @param requireAlive {@code true} to only provide threads currently seen as alive
   * @return A list of known thread references still reachable
   */
  public List<Thread> getThreads(boolean requireAlive) {
    List<Thread> result = new ArrayList<>(threads.size());
    Iterator<WeakReference<Thread>> it = threads.iterator();
    while (it.hasNext()) {
      Thread t = it.next().get();
      if (t == null) {
        it.remove();
      } else if (! requireAlive || t.isAlive()) { 
        result.add(t);
      }
    }
    return result;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread result = super.newThread(r);
    
    if (Clock.lastKnownForwardProgressingMillis() - lastCleanupTime > REFERENCE_QUEUE_CHECK_INTERVAL_MILLIS) {
      // not designed to be a memory barrier, can be done in parallel, just dont want it done too much
      lastCleanupTime = Clock.lastKnownForwardProgressingMillis();
      Iterator<WeakReference<Thread>> it = threads.iterator();
      while (it.hasNext()) {
        if (it.next().get() == null) {
          it.remove();
        }
      }
    }
    
    threads.add(new WeakReference<>(result));
    
    return result;
  }
}

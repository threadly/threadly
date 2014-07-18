package org.threadly.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * <p>Implementation of {@link ThreadFactory} which is configurable for the most common 
 * use cases.  Specifically, it allows you several options for how to prefix the name 
 * of threads, if returned threads should be daemon or not, what their priority should be, 
 * and if an UncaughtExceptionHandler should be provided to new threads.  You can construct 
 * this with no arguments, and it's behavior will match that of 
 * Executors.defaultThreadFactory().</p>
 *  
 * @author jent - Mike Jensen
 * @since 2.3.0
 */
public class ConfigurableThreadFactory implements ThreadFactory {
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = false;
  private static final AtomicInteger NEXT_POOL_NUMBER = new AtomicInteger(1);
  
  protected final ThreadGroup group;
  protected final String threadNamePrefix;
  protected final boolean useDaemonThreads;
  protected final int threadPriority;
  protected final UncaughtExceptionHandler defaultUncaughtExceptionHandler;
  private final AtomicInteger nextThreadNumber;
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} with the default parameters.  Threads 
   * produced from this should behave exactly like Executors.defaultThreadFactory(), except the 
   * pool number provided in the thread name will be respective to the ones created from 
   * other {@link ConfigurableThreadFactory} instances.
   */
  public ConfigurableThreadFactory() {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying the prefix for the name 
   * of newly created threads.  
   * 
   * If specified with true for appendPoolIdToPrefix it will append a unique "pool" id to 
   * the prefix, giving it the format of threadNamePrefix + <UNIQUE_POOL_ID> + "-thread-".  
   * If appendPoolIdToPrefix is specified as false, only a unique thread id will be 
   * appended to the prefix.  In either case, the produced threads name will be appended 
   * with a unique thread id for the factory instance.
   * 
   * @param threadNamePrefix prefix for all threads created
   * @param appendPoolIdToPrefix true to append a unique pool id to the thread prefix
   */
  public ConfigurableThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix) {
    this(threadNamePrefix, appendPoolIdToPrefix, 
         DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying the behavior for if threads 
   * should be daemon or not.
   * 
   * @param useDaemonThreads true if produced threads should be daemon threads
   */
  public ConfigurableThreadFactory(boolean useDaemonThreads) {
    this(null, true, useDaemonThreads, Thread.NORM_PRIORITY, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying the priority for 
   * produced threads.  
   * 
   * If the priority is below or above the max available thread priority, this will be 
   * adjusted to the limit of the system.
   * 
   * @param threadPriority Priority for newly created threads
   */
  public ConfigurableThreadFactory(int threadPriority) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, threadPriority, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying an 
   * {@link UncaughtExceptionHandler} that will be provided to all newly created threads.
   * 
   * @param defaultUncaughtExceptionHandler {@link UncaughtExceptionHandler} to provide to newly created threads
   */
  public ConfigurableThreadFactory(UncaughtExceptionHandler defaultUncaughtExceptionHandler) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, 
         Thread.NORM_PRIORITY, defaultUncaughtExceptionHandler);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} allowing you to provide specific 
   * values for everything which this class allows to be configured.  You must use this 
   * constructor if you need to adjust two or more values.  
   * 
   * If specified with true for appendPoolIdToPrefix it will append a unique "pool" id to 
   * the prefix, giving it the format of threadNamePrefix + <UNIQUE_POOL_ID> + "-thread-".  
   * If appendPoolIdToPrefix is specified as false, only a unique thread id will be 
   * appended to the prefix.  In either case, the produced threads name will be appended 
   * with a unique thread id for the factory instance.
   * 
   * If the priority is below or above the max available thread priority, this will be 
   * adjusted to the limit of the system.
   * 
   * @param threadNamePrefix prefix for all threads created, null to match default
   * @param appendPoolIdToPrefix true to append a unique pool id to the thread prefix, true to match default
   * @param useDaemonThreads true if produced threads should be daemon threads, false to match default
   * @param threadPriority Priority for newly created threads, Thread.NORM_PRIORITY to match default
   * @param uncaughtExceptionHandler UncaughtExceptionHandler to provide to newly created threads, null to match default
   */
  public ConfigurableThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix, 
                                   boolean useDaemonThreads, int threadPriority, 
                                   UncaughtExceptionHandler uncaughtExceptionHandler) {
    if (threadPriority > Thread.MAX_PRIORITY) {
      threadPriority = Thread.MAX_PRIORITY;
    } else if (threadPriority < Thread.MIN_PRIORITY) {
      threadPriority = Thread.MIN_PRIORITY;
    }
    if (threadNamePrefix == null) {
      threadNamePrefix = "pool-";
    }
    if (appendPoolIdToPrefix) {
      threadNamePrefix += NEXT_POOL_NUMBER.getAndIncrement() + "-thread-";
    }
    
    SecurityManager s = System.getSecurityManager();
    if (s != null) {
      this.group = s.getThreadGroup();
    } else {
      this.group = Thread.currentThread().getThreadGroup();
    }
    this.threadNamePrefix = threadNamePrefix;
    this.useDaemonThreads = useDaemonThreads;
    this.threadPriority = threadPriority;
    this.defaultUncaughtExceptionHandler = uncaughtExceptionHandler;
    this.nextThreadNumber = new AtomicInteger(1);
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, 
                          threadNamePrefix + nextThreadNumber.getAndIncrement());
    
    if (t.isDaemon() != useDaemonThreads) {
      t.setDaemon(useDaemonThreads);
    }
    if (t.getPriority() != threadPriority) {
      t.setPriority(threadPriority);
    }
    if (defaultUncaughtExceptionHandler != null) {
      t.setUncaughtExceptionHandler(defaultUncaughtExceptionHandler);
    }
    
    return t;
  }
}

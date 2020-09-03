package org.threadly.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;

/**
 * Implementation of {@link ThreadFactory} which is configurable for the most common use cases.  
 * Specifically, it allows you several options for how to prefix the name of threads, if returned 
 * threads should be daemon or not, what their priority should be, and if an 
 * {@link UncaughtExceptionHandler} should be provided to new threads.  You can construct this 
 * with no arguments, and it's behavior will match that of 
 * {@code Executors.defaultThreadFactory()}.
 * 
 * @since 2.3.0
 */
public class ConfigurableThreadFactory implements ThreadFactory {
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = false;
  private static final AtomicInteger NEXT_POOL_NUMBER = new AtomicInteger(1);
  
  /**
   * Construct a new builder as an alternative to the large full parameter constructor when multiple 
   * features are needing to be configured.
   * <p>
   * Defaults (equivalent to {@link #ConfigurableThreadFactory()}):
   * <ul>
   * <li>{@code threadNamePrefix}: "pool-"
   * <li>{@code appendPoolIdToPrefix}: {@code true}
   * <li>{@code useDaemonThreads}: {@code false}
   * <li>{@code threadPriority}: {@link Thread#NORM_PRIORITY}
   * <li>{@code threadlyExceptionHandler}: {@code null}
   * </ul>
   * 
   * @since 5.39
   * @return A new builder which can be configured then finally constructed using {@code build()}
   */
  public static ConfigurableThreadFactoryBuilder builder() {
    return new ConfigurableThreadFactoryBuilder();
  }
  
  /**
   * Builder for configuring a new {@link ConfigurableThreadFactory}.  When ready invoke 
   * {@link #build()} to construct the new factory.
   * 
   * @since 5.39
   */
  public static class ConfigurableThreadFactoryBuilder {
    protected String threadNamePrefix = null;
    protected boolean appendPoolIdToPrefix = true; 
    protected boolean useDaemonThreads = DEFAULT_NEW_THREADS_DAEMON;
    protected int threadPriority = Thread.NORM_PRIORITY;
    protected ExceptionHandler threadlyExceptionHandler = null;
    protected Consumer<Thread> notifyThreadCreation = null;
    
    /**
     * Call to set the prefix for newly created threads names.  By default this will be 
     * {@code "pool-"}.  See {@link #appendPoolIdToPrefix(boolean)} for determining the behavior 
     * of a pool ID following this prefix. 
     * 
     * @param threadNamePrefix Prefix for thread name or {@code null} to use the default
     * @return {@code this} instance
     */
    public ConfigurableThreadFactoryBuilder threadNamePrefix(String threadNamePrefix) {
      this.threadNamePrefix = threadNamePrefix;
      return this;
    }
    
    /**
     * Sets if a unique pool id should be appended to the prefix of thread names.  By default this 
     * will append a pool id so that all threads are uniquely identifiable by name.
     * 
     * @param appendPoolIdToPrefix True to indicate an auto incrementing pool id should be included in the name
     * @return {@code this} instance
     */
    public ConfigurableThreadFactoryBuilder appendPoolIdToPrefix(boolean appendPoolIdToPrefix) {
      this.appendPoolIdToPrefix = appendPoolIdToPrefix;
      return this;
    }
    
    /**
     * Sets of the created threads should be started with daemon status.  By default threads will 
     * be started in daemon status.
     * 
     * @param useDaemonThreads True if started threads should be set as daemon
     * @return {@code this} instance
     */
    public ConfigurableThreadFactoryBuilder useDaemonThreads(boolean useDaemonThreads) {
      this.useDaemonThreads = useDaemonThreads;
      return this;
    }
    
    /**
     * Sets the priority associated to the thread.  Depending on OS and JVM settings this priority 
     * may be ignored (see java documentation about thread priorities).  This value should be 
     * between {@link Thread#MIN_PRIORITY} and {@link Thread#MAX_PRIORITY}.
     * 
     * @param threadPriority The priority value to be set on new threads
     * @return {@code this} instance
     */
    public ConfigurableThreadFactoryBuilder threadPriority(int threadPriority) {
      this.threadPriority = threadPriority;
      return this;
    }
    
    /**
     * Sets an {@link ExceptionHandler} to be set for these newly created threads.  Typically 
     * {@link ExceptionUtils#setInheritableExceptionHandler(ExceptionHandler)} or 
     * {@link ExceptionUtils#setDefaultExceptionHandler(ExceptionHandler)} are better options.  
     * However this allows you to set an {@link ExceptionHandler} for threads specifically created 
     * from this ThreadFactory.
     * 
     * @param exceptionHandler Handler to be delegated to for errors in produced threads
     * @return {@code this} instance
     */
    public ConfigurableThreadFactoryBuilder exceptionHandler(ExceptionHandler exceptionHandler) {
      this.threadlyExceptionHandler = exceptionHandler;
      return this;
    }

    /**
     * Sets a {@link Consumer} which will be provided Thread references as they are created and 
     * before they are started.  This can be useful when threads need to be provided to another 
     * library or code to monitor.
     * 
     * @param notifyThreadCreation Consumer to accept the Threads as they are created
     * @return {@code this} instance
     */
    public ConfigurableThreadFactoryBuilder onThreadCreation(Consumer<Thread> notifyThreadCreation) {
      this.notifyThreadCreation = notifyThreadCreation;
      return this;
    }
    
    /**
     * Call to construct the {@link ConfigurableThreadFactory} when configuration is ready.
     * 
     * @return Newly constructed ThreadFactory
     */
    public ConfigurableThreadFactory build() {
      return new ConfigurableThreadFactory(threadNamePrefix, appendPoolIdToPrefix, 
                                           useDaemonThreads, threadPriority, null, 
                                           threadlyExceptionHandler, notifyThreadCreation);
    }
  }
  
  protected final ThreadGroup group;
  protected final String threadNamePrefix;
  protected final boolean useDaemonThreads;
  protected final int threadPriority;
  protected final UncaughtExceptionHandler defaultUncaughtExceptionHandler;
  protected final ExceptionHandler defaultThreadlyExceptionHandler;
  protected final Consumer<Thread> notifyThreadCreation;
  private final AtomicInteger nextThreadNumber;
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} with the default parameters.  Threads 
   * produced from this should behave exactly like Executors.defaultThreadFactory(), except the 
   * pool number provided in the thread name will be respective to the ones created from other 
   * {@link ConfigurableThreadFactory} instances.
   */
  public ConfigurableThreadFactory() {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, null, null, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying the prefix for the name of 
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
  public ConfigurableThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix) {
    this(threadNamePrefix, appendPoolIdToPrefix, 
         DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, null, null, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying the behavior for if threads 
   * should be daemon or not.
   * 
   * @param useDaemonThreads {@code true} if produced threads should be daemon threads
   */
  public ConfigurableThreadFactory(boolean useDaemonThreads) {
    this(null, true, useDaemonThreads, Thread.NORM_PRIORITY, null, null, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying the priority for produced 
   * threads.  
   * <p>
   * If the priority is below or above the max available thread priority, this will be adjusted to 
   * the limit of the system.
   * 
   * @param threadPriority Priority for newly created threads
   */
  public ConfigurableThreadFactory(int threadPriority) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, threadPriority, null, null, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying an 
   * {@link UncaughtExceptionHandler} that will be provided to all newly created threads.
   * 
   * @param defaultUncaughtExceptionHandler {@link UncaughtExceptionHandler} to provide to newly created threads
   */
  public ConfigurableThreadFactory(UncaughtExceptionHandler defaultUncaughtExceptionHandler) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, 
         Thread.NORM_PRIORITY, defaultUncaughtExceptionHandler, null, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying an {@link ExceptionHandler} 
   * that will be provided to all newly created threads.
   * 
   * @param defaultThreadlyExceptionHandler {@link ExceptionHandler} to provide to newly created threads
   */
  public ConfigurableThreadFactory(ExceptionHandler defaultThreadlyExceptionHandler) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, 
         Thread.NORM_PRIORITY, null, defaultThreadlyExceptionHandler, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} specifying a {@link Consumer} that will be 
   * provided threads as they created.
   * 
   * @param notifyThreadCreation Consumer to be provided whenever a new thread is about to be returned or {@code null}
   */
  public ConfigurableThreadFactory(Consumer<Thread> notifyThreadCreation) {
    this(null, true, DEFAULT_NEW_THREADS_DAEMON, Thread.NORM_PRIORITY, 
         null, null, notifyThreadCreation);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} allowing you to provide specific values 
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
   * @deprecated Replaced by constructor which accepts a {@link Consumer} for created threads.  
   *               Specifying {@code null} will provide a direct replacement, or alternatively a 
   *               builder can be used from {@link #builder()}.
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
  @Deprecated
  public ConfigurableThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix, 
                                   boolean useDaemonThreads, int threadPriority, 
                                   UncaughtExceptionHandler uncaughtExceptionHandler, 
                                   ExceptionHandler defaultThreadlyExceptionHandler) {
    this(threadNamePrefix, appendPoolIdToPrefix, useDaemonThreads, threadPriority, 
         uncaughtExceptionHandler, defaultThreadlyExceptionHandler, null);
  }
  
  /**
   * Constructs a new {@link ConfigurableThreadFactory} allowing you to provide specific values 
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
   * @param notifyThreadCreation Consumer to be provided whenever a new thread is about to be returned or {@code null}
   */
  public ConfigurableThreadFactory(String threadNamePrefix, boolean appendPoolIdToPrefix, 
                                   boolean useDaemonThreads, int threadPriority, 
                                   UncaughtExceptionHandler uncaughtExceptionHandler, 
                                   ExceptionHandler defaultThreadlyExceptionHandler, 
                                   Consumer<Thread> notifyThreadCreation) {
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
    this.defaultThreadlyExceptionHandler = defaultThreadlyExceptionHandler;
    this.notifyThreadCreation = notifyThreadCreation;
    this.nextThreadNumber = new AtomicInteger(1);
  }

  @Override
  public Thread newThread(Runnable r) {
    if (defaultThreadlyExceptionHandler != null) {
      r = new ExceptionHandlerSettingRunnable(defaultThreadlyExceptionHandler, r);
    }
    Thread t = new Thread(group, r, threadNamePrefix + nextThreadNumber.getAndIncrement());
    
    if (t.isDaemon() != useDaemonThreads) {
      t.setDaemon(useDaemonThreads);
    }
    if (t.getPriority() != threadPriority) {
      t.setPriority(threadPriority);
    }
    if (defaultUncaughtExceptionHandler != null) {
      t.setUncaughtExceptionHandler(defaultUncaughtExceptionHandler);
    }
    if (notifyThreadCreation != null) {
      notifyThreadCreation.accept(t);
    }
    
    return t;
  }
  
  /**
   * Because the {@link ExceptionHandler} can not be set before the thread is started.  We must 
   * wrap it in this implementation to set the handler before the runnable actually starts.
   * 
   * @since 2.4.0
   */
  protected static class ExceptionHandlerSettingRunnable implements Runnable, RunnableContainer {
    private final ExceptionHandler exceptionHandler;
    private final Runnable toRun;
    
    protected ExceptionHandlerSettingRunnable(ExceptionHandler exceptionHandler, Runnable r) {
      this.exceptionHandler = exceptionHandler;
      toRun = r;
    }

    @Override
    public void run() {
      ExceptionUtils.setThreadExceptionHandler(exceptionHandler);
      toRun.run();
    }

    @Override
    public Runnable getContainedRunnable() {
      return toRun;
    }
  }
}

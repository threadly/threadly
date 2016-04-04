package org.threadly.concurrent.statistics;

import java.util.concurrent.ThreadFactory;

import org.threadly.concurrent.TaskPriority;

/**
 * <p>An implementation of {@link org.threadly.concurrent.SingleThreadScheduler} which tracks run 
 * and usage statistics.  This is designed for testing and troubleshooting.  It has a little more 
 * overhead from the normal {@link org.threadly.concurrent.SingleThreadScheduler}.</p>
 * 
 * <p>It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.</p>
 * 
 * @deprecated Moved to org.threadly.concurrent.statistic.SingleThreadSchedulerStatisticTracker
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0 (earlier forms existed since 1.0.0)
 */
@Deprecated
public class SingleThreadSchedulerStatisticTracker 
                 extends org.threadly.concurrent.statistic.SingleThreadSchedulerStatisticTracker 
                 implements StatisticPriorityScheduler {
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   */
  public SingleThreadSchedulerStatisticTracker() {
    super();
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs) {
    super(defaultPriority, maxWaitForLowPriorityInMs);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadSchedulerStatisticTracker(boolean daemonThread) {
    super(daemonThread);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               boolean daemonThread) {
    super(defaultPriority, maxWaitForLowPriorityInMs, daemonThread);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadSchedulerStatisticTracker(ThreadFactory threadFactory) {
    super(threadFactory);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               ThreadFactory threadFactory) {
    super(defaultPriority, maxWaitForLowPriorityInMs, threadFactory);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(int maxStatisticWindowSize) {
    super(maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               int maxStatisticWindowSize) {
    super(defaultPriority, maxWaitForLowPriorityInMs, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(boolean daemonThread, int maxStatisticWindowSize) {
    super(daemonThread, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, boolean daemonThread, 
                                               int maxStatisticWindowSize) {
    super(defaultPriority, maxWaitForLowPriorityInMs, daemonThread, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize) {
    super(threadFactory, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize) {
    super(defaultPriority, maxWaitForLowPriorityInMs, threadFactory, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.  
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(int maxStatisticWindowSize, boolean accurateTime) {
    super(maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  This defaults to using a daemon thread for the 
   * scheduler.  
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    super(defaultPriority, maxWaitForLowPriorityInMs, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(boolean daemonThread, int maxStatisticWindowSize, 
                                               boolean accurateTime) {
    super(daemonThread, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, boolean daemonThread, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    super(defaultPriority, maxWaitForLowPriorityInMs, daemonThread, 
         maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    super(threadFactory, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link org.threadly.concurrent.SingleThreadScheduler}.  No threads will 
   * start until the first task is provided.  
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    super(defaultPriority, maxWaitForLowPriorityInMs, threadFactory, 
          maxStatisticWindowSize, accurateTime);
  }
}

package org.threadly.concurrent;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.AbstractPriorityScheduler.AccurateRecurringDelayTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.AccurateRecurringRateTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueSet;
import org.threadly.concurrent.AbstractPriorityScheduler.TaskWrapper;
import org.threadly.concurrent.NoThreadScheduler.NoThreadRecurringDelayTaskWrapper;
import org.threadly.concurrent.NoThreadScheduler.NoThreadRecurringRateTaskWrapper;
import org.threadly.util.Clock;

/**
 * PLEASE IGNORE THIS CLASS, DO NOT USE
 * <p>
 * This internal class is only used for threadly out of package wrappers to be able to directly 
 * access protected functionality.  This is primarily done to avoid excessive checking, or short 
 * cut logic in order to get performance gains.
 * <p>
 * Because of that, this is highly specialized and not likely to be useful to anyone else.
 * 
 * @since 4.6.0
 */
public final class ThreadlyInternalAccessor {
  /**
   * Used for gaining compatibility with java.util.concurrent when a {@link Delayed} object is 
   * needed.
   * 
   * @param pScheduler Scheduler to submit task to
   * @param task Task to be submitted
   * @param priority Priority for task to be submitted at
   * @param delayInMillis Delay for task execution
   * @return Delayed implementation which corresponds to executed task
   */
  public static Delayed doScheduleAndGetDelayed(AbstractPriorityScheduler pScheduler, Runnable task, 
                                                TaskPriority priority, long delayInMillis) {
    return new DelayedTaskWrapper(pScheduler.doSchedule(task, delayInMillis, priority));
  }
  
  /**
   * Used for gaining compatibility with java.util.concurrent when a {@link Delayed} object is 
   * needed.
   * 
   * @param pScheduler Scheduler to submit task to
   * @param task Task to be submitted
   * @param priority Priority for task to be submitted at
   * @param initialDelay initial delay for task to execute
   * @param periodInMillis recurring delay for task to execute
   * @return Delayed implementation which corresponds to executed task
   */
  public static Delayed doScheduleAtFixedRateAndGetDelayed(PriorityScheduler pScheduler, 
                                                           Runnable task, TaskPriority priority, 
                                                           long initialDelay, long periodInMillis) {
    QueueSet queueSet = pScheduler.queueManager.getQueueSet(priority);
    AccurateRecurringRateTaskWrapper rrtw = 
        new AccurateRecurringRateTaskWrapper(task, queueSet,
                                             Clock.accurateForwardProgressingMillis() + initialDelay, 
                                             periodInMillis);
    pScheduler.queueScheduled(queueSet, rrtw);
    return new DelayedTaskWrapper(rrtw);
  }
  
  /**
   * Used for gaining compatibility with java.util.concurrent when a {@link Delayed} object is 
   * needed.
   * 
   * @param pScheduler Scheduler to submit task to
   * @param task Task to be submitted
   * @param priority Priority for task to be submitted at
   * @param initialDelay initial delay for task to execute
   * @param delayInMs recurring delay for task to execute
   * @return Delayed implementation which corresponds to executed task
   */
  public static Delayed doScheduleWithFixedDelayAndGetDelayed(PriorityScheduler pScheduler, 
                                                              Runnable task, TaskPriority priority, 
                                                              long initialDelay, long delayInMs) {
    QueueSet queueSet = pScheduler.queueManager.getQueueSet(priority);
    AccurateRecurringDelayTaskWrapper rdtw = 
        new AccurateRecurringDelayTaskWrapper(task, queueSet,
                                             Clock.accurateForwardProgressingMillis() + initialDelay, 
                                             delayInMs);
    pScheduler.queueScheduled(queueSet, rdtw);
    return new DelayedTaskWrapper(rdtw);
  }
  
  /**
   * Used for gaining compatibility with java.util.concurrent when a {@link Delayed} object is 
   * needed.
   * 
   * @param scheduler Scheduler to submit task to
   * @param task Task to be submitted
   * @param priority Priority for task to be submitted at
   * @param initialDelay initial delay for task to execute
   * @param periodInMillis recurring delay for task to execute
   * @return Delayed implementation which corresponds to executed task
   */
  public static Delayed doScheduleAtFixedRateAndGetDelayed(SingleThreadScheduler scheduler, 
                                                           Runnable task, TaskPriority priority, 
                                                           long initialDelay, long periodInMillis) {
    NoThreadScheduler nts = scheduler.getRunningScheduler();
    QueueSet queueSet = nts.queueManager.getQueueSet(priority);
    NoThreadRecurringRateTaskWrapper rt = 
        nts.new NoThreadRecurringRateTaskWrapper(task, queueSet, 
                                                 Clock.accurateForwardProgressingMillis() + 
                                                   initialDelay, 
                                                 periodInMillis);
    queueSet.addScheduled(rt);
    return new DelayedTaskWrapper(rt);
  }
  
  /**
   * Used for gaining compatibility with java.util.concurrent when a {@link Delayed} object is 
   * needed.
   * 
   * @param scheduler Scheduler to submit task to
   * @param task Task to be submitted
   * @param priority Priority for task to be submitted at
   * @param initialDelay initial delay for task to execute
   * @param delayInMs recurring delay for task to execute
   * @return Delayed implementation which corresponds to executed task
   */
  public static Delayed doScheduleWithFixedDelayAndGetDelayed(SingleThreadScheduler scheduler, 
                                                              Runnable task, TaskPriority priority, 
                                                              long initialDelay, long delayInMs) {
    NoThreadScheduler nts = scheduler.getRunningScheduler();
    QueueSet queueSet = nts.queueManager.getQueueSet(priority);
    NoThreadRecurringDelayTaskWrapper rdt = 
        nts.new NoThreadRecurringDelayTaskWrapper(task, queueSet, 
                                                  Clock.accurateForwardProgressingMillis() + 
                                                    initialDelay, 
                                                  delayInMs);
    queueSet.addScheduled(rdt);
    return new DelayedTaskWrapper(rdt);
  }
  
  /**
   * Small wrapper to convert from a {@link PriorityScheduler.TaskWrapper} into a Delayed 
   * interface.
   * 
   * @since 4.6.0
   */
  protected static final class DelayedTaskWrapper implements Delayed {
    private final TaskWrapper task;
    
    public DelayedTaskWrapper(TaskWrapper task) {
      this.task = task;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(task.getRunTime() - Clock.accurateForwardProgressingMillis(), 
                          TimeUnit.MILLISECONDS);
    }
    
    @Override
    public int compareTo(Delayed o) {
      if (this == o) {
        return 0;
      } else if (o instanceof DelayedTaskWrapper) {
        return (int)(task.getRunTime() - ((DelayedTaskWrapper)o).task.getRunTime());
      } else {
        long thisDelay = this.getDelay(TimeUnit.MILLISECONDS);
        long otherDelay = o.getDelay(TimeUnit.MILLISECONDS);
        if (thisDelay == otherDelay) {
          return 0;
        } else if (thisDelay > otherDelay) {
          return 1;
        } else {
          return -1;
        }
      }
    }
  }
}

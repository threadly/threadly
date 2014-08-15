package org.threadly.concurrent;

import org.threadly.concurrent.lock.StripedLock;

/**
 * <p>This is a class which is more full featured than {@link TaskExecutorDistributor}, 
 * but it does require a scheduler implementation in order to be able to perform scheduling.</p>
 * 
 * <p>The same guarantees and restrictions for the {@link TaskExecutorDistributor} also exist 
 * for this class.  Please read the javadoc for {@link TaskExecutorDistributor} to understand 
 * more about how this operates.</p>
 * 
 * @deprecated use replacement at org.threadly.concurrent.KeyDistributedScheduler
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class TaskSchedulerDistributor extends KeyDistributedScheduler {
  /**
   * Constructor to use a provided scheduler implementation for running tasks.  
   * 
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the "getTaskQueueSize" call (thus preferring 
   * high performance).
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler) {
    super(scheduler);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.  
   * 
   * This constructor allows you to specify if you want accurate queue sizes to be 
   * tracked for given thread keys.  There is a performance hit associated with this, 
   * so this should only be enabled if "getTaskQueueSize" calls will be used.  
   * 
   * This constructs with a default expected level of concurrency of 16.
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, boolean accurateQueueSize) {
    super(scheduler, accurateQueueSize);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.  
   * 
   * This also allows you to specify if you want accurate queue sizes to be tracked for 
   * given thread keys.  There is a performance hit associated with this, so this should 
   * only be enabled if "getTaskQueueSize" calls will be used.  
   * 
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the "getTaskQueueSize" call (thus preferring 
   * high performance).
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, int maxTasksPerCycle) {
    super(scheduler, maxTasksPerCycle);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.  
   * 
   * This also allows you to specify if you want accurate queue sizes to be tracked for given 
   * thread keys.  There is a performance hit associated with this, so this should only be 
   * enabled if "getTaskQueueSize" calls will be used.
   * 
   * This constructs with a default expected level of concurrency of 16. 
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, int maxTasksPerCycle, 
                                  boolean accurateQueueSize) {
    super(scheduler, maxTasksPerCycle, accurateQueueSize);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor does not attempt to have an accurate queue size for the 
   * "getTaskQueueSize" call (thus preferring high performance).
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel. 
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler) {
    super(expectedParallism, scheduler);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to specify if you want accurate queue sizes to be 
   * tracked for given thread keys.  There is a performance hit associated with this, 
   * so this should only be enabled if "getTaskQueueSize" calls will be used.
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel. 
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                  boolean accurateQueueSize) {
    super(expectedParallism, scheduler, accurateQueueSize);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * This constructor does not attempt to have an accurate queue size for the 
   * "getTaskQueueSize" call (thus preferring high performance).
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                  int maxTasksPerCycle) {
    super(expectedParallism, scheduler, maxTasksPerCycle);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * This also allows you to specify if you want accurate queue sizes to be 
   * tracked for given thread keys.  There is a performance hit associated with this, 
   * so this should only be enabled if "getTaskQueueSize" calls will be used.
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                  int maxTasksPerCycle, boolean accurateQueueSize) {
    super(expectedParallism, scheduler, maxTasksPerCycle, accurateQueueSize);
  }
  
  /**
   * Constructor to be used in unit tests.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * @param scheduler scheduler to be used for task worker execution 
   * @param sLock lock to be used for controlling access to workers
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  protected TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, StripedLock sLock, 
                                     int maxTasksPerCycle, boolean accurateQueueSize) {
    super(scheduler, sLock, maxTasksPerCycle, accurateQueueSize);
  }
}

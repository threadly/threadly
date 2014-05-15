package org.threadly.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * In order to avoid a performance hit by verifying state which would indicate a programmer 
 * error at runtime.  This class functions to verify those little things during unit tests.  
 * For that reason this class extends {@link PriorityScheduledExecutor} to do additional 
 * functions, but calls into the super functions to verify the actual behavior. 
 * 
 * @author jent - Mike Jensen
 */
public class StrictPriorityScheduledExecutor extends PriorityScheduledExecutor {
  @SuppressWarnings("javadoc")
  public StrictPriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                         long keepAliveTimeInMs) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs);
  }

  @SuppressWarnings("javadoc")
  public StrictPriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                         long keepAliveTimeInMs, boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, useDaemonThreads);
  }

  @SuppressWarnings("javadoc")
  public StrictPriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                         long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                         long maxWaitForLowPriorityInMs) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs);
  }

  @SuppressWarnings("javadoc")
  public StrictPriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                         long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                         long maxWaitForLowPriorityInMs, 
                                         final boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, 
          useDaemonThreads);
  }

  @SuppressWarnings("javadoc")
  public StrictPriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                         long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                         long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, 
          threadFactory);
  }
  
  private void verifyWorkersLock() {
    if (! Thread.holdsLock(workersLock)) {
      throw new IllegalStateException("Workers lock must be held before calling");
    }
  }

  @Override
  protected Worker makeNewWorker() {
    verifyWorkersLock();
    
    return super.makeNewWorker();
  }

  @Override
  protected Worker getExistingWorker(long maxWaitTimeInMs) throws InterruptedException {
    verifyWorkersLock();
    
    return super.getExistingWorker(maxWaitTimeInMs);
  }
}

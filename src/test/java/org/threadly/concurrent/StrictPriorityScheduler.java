package org.threadly.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * In order to avoid a performance hit by verifying state which would indicate a programmer 
 * error at runtime.  This class functions to verify those little things during unit tests.  
 * For that reason this class extends {@link PriorityScheduler} to do additional 
 * functions, but calls into the super functions to verify the actual behavior. 
 * 
 * @author jent - Mike Jensen
 */
@SuppressWarnings("javadoc")
public class StrictPriorityScheduler extends PriorityScheduler {
  public StrictPriorityScheduler(int corePoolSize, int maxPoolSize, long keepAliveTimeInMs) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs);
  }

  public StrictPriorityScheduler(int corePoolSize, int maxPoolSize,
                                 long keepAliveTimeInMs, boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, useDaemonThreads);
  }

  public StrictPriorityScheduler(int corePoolSize, int maxPoolSize,
                                 long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                 long maxWaitForLowPriorityInMs) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs);
  }

  public StrictPriorityScheduler(int corePoolSize, int maxPoolSize,
                                 long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                 long maxWaitForLowPriorityInMs, boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, 
          useDaemonThreads);
  }

  public StrictPriorityScheduler(int corePoolSize, int maxPoolSize,
                                 long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                 long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    super(new StrictWorkerPool(threadFactory, corePoolSize, maxPoolSize, keepAliveTimeInMs, maxWaitForLowPriorityInMs), 
          defaultPriority);
  }
  
  protected static class StrictWorkerPool extends WorkerPool {
    protected StrictWorkerPool(ThreadFactory threadFactory, int corePoolSize, int maxPoolSize,
                               long keepAliveTimeInMs, long maxWaitForLorPriorityInMs) {
      super(threadFactory, corePoolSize, maxPoolSize, keepAliveTimeInMs, maxWaitForLorPriorityInMs);
    }

    private void verifyWorkersLock() {
      if (! Thread.holdsLock(workersLock)) {
        throw new IllegalStateException("Workers lock must be held before calling");
      }
    }

    @Override
    protected Worker makeNewWorker() {
      verifyWorkersLock();
      
      return new StrictWorkerWrapper(this, threadFactory, super.makeNewWorker());
    }
  
    @Override
    protected Worker getExistingWorker(long maxWaitTimeInMs) throws InterruptedException {
      verifyWorkersLock();
      
      return super.getExistingWorker(maxWaitTimeInMs);
    }
  }
  
  /**
   * <p>This is a hack...I did not want to extract worker into an interface, so we extend the 
   * class and just override all implemented functions.  We can then defer to the provided 
   * deligate for any operations needed.  This is because, 1, I did not want to add an interface, 
   * and 2, I did not want to change the visibility of {@code currentPoolSize}</p>
   * 
   * <p>I know this is ugly, but since it is only used in test code, I don't mind it.  I would 
   * NEVER do this in the main code base (I am only doing this to keep the main code base small and 
   * clean).</p>
   * 
   * @author jent - Mike Jensen
   */
  protected static class StrictWorkerWrapper extends Worker {
    private final Worker deligateWorker;
    
    private StrictWorkerWrapper(WorkerPool workerPool, ThreadFactory threadFactory, 
                                Worker deligateWorker) {
      super(workerPool, threadFactory);
      
      this.deligateWorker = deligateWorker;
    }

    @Override
    public void start() {
      deligateWorker.start();
    }
    
    @Override
    public boolean startIfNotStarted() {
      return deligateWorker.startIfNotStarted();
    }
    
    @Override
    public void stop() {
      deligateWorker.stop();
    }
    
    @Override
    public boolean stopIfRunning() {
      return deligateWorker.stopIfRunning();
    }

    @Override
    protected void startupService() {
      // overriding above functions should mean this is never called
      throw new UnsupportedOperationException();
    }

    @Override
    protected void shutdownService() {
      // overriding above functions should mean this is never called
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void run() {
      // overriding above functions should mean this is never called
      throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isRunning() {
      return deligateWorker.isRunning();
    }
    
    @Override
    public void nextTask(Runnable task) {
      if (! deligateWorker.isRunning()) {
        throw new IllegalStateException();
      } else if (deligateWorker.nextTask != null) {
        throw new IllegalStateException();
      }
      
      deligateWorker.nextTask(task);
    }
    
    @Override
    public long getLastRunTime() {
      return deligateWorker.getLastRunTime();
    }
  }
}

package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.VirtualCallable;

/**
 * Abstract limiter for any implementations which need to schedule and handle futures.
 * 
 * @author jent - Mike Jensen
 */
abstract class AbstractSchedulerLimiter extends AbstractThreadPoolLimiter {
  /**
   * Constructor for abstract class to call into for anyone extending this class.
   * 
   * @param maxConcurrency maximum qty of runnables to run in parallel
   * @param subPoolName name to give threads while tasks running in pool (null to not change thread names)
   */
  protected AbstractSchedulerLimiter(int maxConcurrency, String subPoolName) {
    super(maxConcurrency, subPoolName);
  }
  
  /**
   * Generic wrapper for callables which are used within the limiters.
   * 
   * @author jent - Mike Jensen
   * @param <T> type for return of callable contained within wrapper
   */
  protected class LimiterCallableWrapper<T> extends VirtualCallable<T> {
    private final Callable<T> callable;
    
    public LimiterCallableWrapper(Callable<T> callable) {
      this.callable = callable;
    }
    
    @Override
    public T call() throws Exception {
      Thread currentThread = null;
      String originalThreadName = null;
      if (subPoolName != null) {
        currentThread = Thread.currentThread();
        originalThreadName = currentThread.getName();
        
        currentThread.setName(makeSubPoolThreadName(originalThreadName));
      }
      
      try {
        if (factory != null && 
            callable instanceof VirtualCallable) {
          VirtualCallable<T> vc = (VirtualCallable<T>)callable;
          return vc.call(factory);
        } else {
          return callable.call();
        }
      } finally {
        handleTaskFinished();
          
        if (subPoolName != null) {
          currentThread.setName(originalThreadName);
        }
      }
    }
  }
}

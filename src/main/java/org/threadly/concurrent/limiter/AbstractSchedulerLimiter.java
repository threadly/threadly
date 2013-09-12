package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.VirtualCallable;
import org.threadly.concurrent.future.FutureFuture.TaskCanceler;
import org.threadly.concurrent.future.StaticCancellationException;

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
  protected class LimiterCallableWrapper<T> extends VirtualCallable<T>
                                            implements TaskCanceler {
    private final Callable<T> callable;
    private final AtomicInteger runStatus;  // 0 = not started, -1 = canceled, 1 = running
    
    public LimiterCallableWrapper(Callable<T> callable) {
      this.callable = callable;
      runStatus = new AtomicInteger(0);
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
        if (runStatus.compareAndSet(0, 1)) {
          if (factory != null && 
              callable instanceof VirtualCallable) {
            VirtualCallable<T> vc = (VirtualCallable<T>)callable;
            return vc.call(factory);
          } else {
            return callable.call();
          }
        } else {
          throw StaticCancellationException.instance();
        }
      } finally {
        runStatus.compareAndSet(1, 0);
        handleTaskFinished();
          
        if (subPoolName != null) {
          currentThread.setName(originalThreadName);
        }
      }
    }

    @Override
    public boolean cancel() {
      return runStatus.compareAndSet(0, -1);
    }
  }
}

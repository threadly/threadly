package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.CallableContainerInterface;
import org.threadly.concurrent.RunnableContainerInterface;
import org.threadly.concurrent.VirtualCallable;
import org.threadly.concurrent.future.FutureFuture;
import org.threadly.concurrent.future.FutureListenableFuture;
import org.threadly.concurrent.future.StaticCancellationException;

/**
 * <p>Abstract limiter for any implementations which need to schedule and handle futures.</p>
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
  
  // TODO - can this class be avoided by turning callables into ListenableFutureTasks sooner?
  /**
   * <p>Generic wrapper for callables which are used within the limiters.</p>
   * 
   * @author jent - Mike Jensen
   * @param <T> type for return of callable contained within wrapper
   */
  protected class LimiterCallableWrapper<T> extends VirtualCallable<T>
                                            implements FutureFuture.TaskCanceler, 
                                                       CallableContainerInterface<T>, 
                                                       RunnableContainerInterface {
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

    @Override
    public Runnable getContainedRunnable() {
      if (callable instanceof RunnableContainerInterface) {
        return ((RunnableContainerInterface)callable).getContainedRunnable();
      } else {
        return null;
      }
    }

    @Override
    public Callable<T> getContainedCallable() {
      return callable;
    }
  }
  
  /**
   * <p>Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class RunnableFutureWrapper extends LimiterRunnableWrapper
                                        implements Wrapper  {
    private final FutureListenableFuture<?> future;
    
    public RunnableFutureWrapper(Runnable runnable, 
                                 FutureListenableFuture<?> future) {
      super(runnable);
      
      this.future = future;
    }
    
    @Override
    protected void doAfterRunTasks() {
      // nothing to do here
    }

    @Override
    public boolean isCallable() {
      return false;
    }

    @Override
    public FutureListenableFuture<?> getFuture() {
      return future;
    }

    @Override
    public boolean hasFuture() {
      return future != null;
    }

    @Override
    public Callable<?> getCallable() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Runnable getRunnable() {
      return this;
    }
  }

  // TODO - can this class be avoided by turning callables into ListenableFutureTasks sooner?
  /**
   * <p>Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.</p>
   * 
   * @author jent - Mike Jensen
   * @param <T> type for return of callable contained within wrapper
   */
  protected class CallableFutureWrapper<T> extends LimiterCallableWrapper<T>
                                           implements Wrapper {
    private final FutureListenableFuture<?> future;
    
    public CallableFutureWrapper(Callable<T> callable, 
                                 FutureListenableFuture<?> future) {
      super(callable);
      
      this.future = future;
    }

    @Override
    public boolean isCallable() {
      return true;
    }

    @Override
    public FutureListenableFuture<?> getFuture() {
      return future;
    }

    @Override
    public boolean hasFuture() {
      return future != null;
    }

    @Override
    public Callable<?> getCallable() {
      return this;
    }

    @Override
    public Runnable getRunnable() {
      throw new UnsupportedOperationException();
    }
  }
  
  /**
   * <p>Interface so that we can handle both callables and runnables.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected interface Wrapper {
    public boolean isCallable();
    public FutureListenableFuture<?> getFuture();
    public boolean hasFuture();
    public Callable<?> getCallable();
    public Runnable getRunnable();
  }
}

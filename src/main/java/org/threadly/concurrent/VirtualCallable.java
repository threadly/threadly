package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * This class helps assist in making concurrent code testable.
 * This class is not strictly required, but it makes the TestablePriorityScheduler
 * a drop in replacement instead of having to pass a LockFactory into your code.
 * 
 * The alternative to using this class would be to pass a LockFactory into your code.
 * But in addition if the runnable needs to sleep, it must do it on the scheduler or locks.
 * 
 * @author jent - Mike Jensen
 * 
 * @param <T> type to be returned from .call()
 */
public abstract class VirtualCallable<T> implements Callable<T>  {
  protected LockFactory factory = null;
  
  /**
   * This is the run call that will be called for schedulers aware of 
   * VirtualRunnable (currently only TestablePriorityScheduler).  This
   * lets us inject a lock factory that works for the given scheduler.
   *  
   * @param factory factory to use while running this runnable
   * @return result from .call()
   * @throws Exception possible thrown from .call()
   */
  public T call(LockFactory factory) throws Exception {
    this.factory = factory;
    
    try {
      return call();
    } finally {
      this.factory = null;
    }
  }
  
  /**
   * Returns a virtual lock for the runnable that makes sense for the 
   * processing thread pool.
   * 
   * @return {@link VirtualLock} to synchronize on and use with pleasure
   */
  protected VirtualLock makeLock() {
    if (factory == null) {
      return new NativeLock();
    } else {
      return factory.makeLock();
    }
  }
  
  /**
   * Alternative to Thread.sleep().  This call defaults to the native 
   * version or uses a method that makes sense for the running thread pool.
   * 
   * You have to use this instead of Thread.sleep() or you will block the 
   * TestablePriorityScheduler.
   * 
   * @param sleepTime Time to pause thread
   * @throws InterruptedException
   */
  protected void sleep(long sleepTime) throws InterruptedException {
    if (factory == null) {
      Thread.sleep(sleepTime);
    } else {
      factory.makeLock().sleep(sleepTime);
    }
  }
  
  /**
   * Constructs a VirtualCallable for a given runnable and result.  This is very similar to 
   * Executors.callable(Runnable, Result).  The difference is this callable implementation is 
   * able to handle when {@link VirtualRunnable} are being provided.
   * 
   * @param task runnable or {@link VirtualRunnable} implementation to call
   * @param result result to be provided from the callable after the runnable has run
   * @return {@link VirtualCallable} object which will run the provided task
   */
  public static <T> VirtualCallable<T> fromRunnable(final Runnable task, 
                                                    final T result) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task to be run within the callable");
    }
    
    return new VirtualCallable<T>() {
      @Override
      public T call() throws Exception {
        if (factory != null && task instanceof VirtualRunnable) {
          ((VirtualRunnable)task).run(factory);
        } else {
          task.run();
        }
        
        return result;
      }
    };
  }
}

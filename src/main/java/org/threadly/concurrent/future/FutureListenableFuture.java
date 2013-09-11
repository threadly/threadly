package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.threadly.util.ExceptionUtils;

/**
 * ListenableFuture implementation which contains a parent {@link ListenableFuture}.
 * This allows a future to be returned before an executor has actually produced a future 
 * which this class will rely on.
 * 
 * @author jent - Mike Jensen
 * @param <T> result type returned by .get()
 */
public class FutureListenableFuture<T> extends FutureFuture<T> 
                                       implements ListenableFuture<T> {
  private ListenableFuture<?> parentFuture;
  
  /**
   * Constructs a new {@link ListenableFuture} instance which will 
   * depend on a ListenableFuture instance to be provided later.
   */
  public FutureListenableFuture() {
    parentFuture = null;
  }
  
  @Override
  public void setParentFuture(Future<?> parentFuture) {
    if (parentFuture instanceof ListenableFuture) {
      setParentFuture((ListenableFuture<?>)parentFuture);
    } else {
      throw new IllegalArgumentException("Can not set with a future which does not implement ListenableFuture");
    }
  }
  
  /**
   * Once the parent ListenableFuture is available, this function should 
   * be called to provide it.
   * 
   * @param parentFuture ListenableFuture instance to depend on.
   */
  public void setParentFuture(ListenableFuture<?> parentFuture) {
    if (this.parentFuture != null) {
      throw new IllegalStateException("Parent future has already been set");
    } else if (parentFuture == null) {
      throw new IllegalArgumentException("Must provide a non-null parent future");
    }
    
    synchronized (this) {
      this.parentFuture = parentFuture;
      
      super.setParentFuture(parentFuture);  // parent will call this.notifyAll()
    }
  }

  @Override
  public void addListener(Runnable listener) {
    addListener(listener, null);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    synchronized (this) {
      while (parentFuture == null) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          throw ExceptionUtils.makeRuntime(e);
        }
      }
    }
    
    parentFuture.addListener(listener, executor);
  }
}
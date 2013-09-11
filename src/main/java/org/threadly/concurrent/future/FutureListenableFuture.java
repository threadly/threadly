package org.threadly.concurrent.future;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

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
  private final List<WaitingListener> waitingToAddListeners;
  private ListenableFuture<?> parentFuture;
  
  /**
   * Constructs a new {@link ListenableFuture} instance which will 
   * depend on a ListenableFuture instance to be provided later.
   */
  public FutureListenableFuture() {
    super();
    
    waitingToAddListeners = new LinkedList<WaitingListener>();
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
      Iterator<WaitingListener> it = waitingToAddListeners.iterator();
      while (it.hasNext()) {
        WaitingListener wl = it.next();
        parentFuture.addListener(wl.listener, wl.executor);
      }
      waitingToAddListeners.clear();
      
      super.setParentFuture(parentFuture);
    }
  }

  @Override
  public void addListener(Runnable listener) {
    addListener(listener, null);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    if (listener == null) {
      throw new IllegalArgumentException("Can not provide a null listener runnable");
    }
    
    synchronized (this) {
      if (parentFuture == null) {
        waitingToAddListeners.add(new WaitingListener(listener, executor));
      } else {
        parentFuture.addListener(listener, executor);
      }
    }
  }
  
  /**
   * Small structure just to hold listeners that will be registered once 
   * the parent future is provided.
   * 
   * @author jent - Mike Jensen
   */
  private class WaitingListener {
    private final Runnable listener;
    private final Executor executor;
    
    private WaitingListener(Runnable listener, Executor executor) {
      this.listener = listener;
      this.executor = executor;
    }
  }
}
package org.threadly.concurrent.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.threadly.concurrent.ContainerHelper;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;

/**
 * <p>Class which assist with holding and calling to Runnable listeners.  In parallel designs it 
 * is common to have things subscribe for actions to occur (to later be alerted once an action 
 * occurs).  This class makes it easy to allow things to register as a listener.</p>
 * 
 * <p>For listener designs which are not using runnables, look at {@link ListenerHelper}.  
 * {@link ListenerHelper} allows you to create similar designs while using any any interface to 
 * call back on.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0 (existed since 1.1.0 as org.threadly.concurrent.ListenerHelper)
 */
public class RunnableListenerHelper {
  protected final Object listenersLock;
  protected final boolean callOnce;
  protected final AtomicBoolean done;
  protected List<Pair<Runnable, Executor>> listeners;
  
  /**
   * Constructs a new {@link RunnableListenerHelper}.  This can call listeners only once, or every 
   * time {@link #callListeners()} is called.
   * 
   * @param callListenersOnce {@code true} if listeners should only be called once
   */
  public RunnableListenerHelper(boolean callListenersOnce) {
    this.listenersLock = new Object();
    this.callOnce = callListenersOnce;
    this.done = new AtomicBoolean(false);
    this.listeners = null;
  }
  
  /**
   * Return a collection of the currently subscribed listener instances.  This returned collection 
   * can NOT be modified.
   * 
   * @return A non-null collection of currently subscribed listeners
   */
  public Collection<Runnable> getSubscribedListeners() {
    synchronized (listenersLock) {
      if (listeners == null) {
        return Collections.emptyList();
      } else {
        return Collections.unmodifiableList(Pair.collectLeft(listeners));
      }
    }
  }
  
  /**
   * Will call all listeners that are registered with this helper.  If any listeners were provided 
   * without an executor, they will execute in the calling thread.  No exceptions will be thrown 
   * in this calling thread if any exceptions occur from the listeners.
   * 
   * If calling multiple times, this will only have an effect if constructed with a {@code false}, 
   * indicating that listeners can expect to be called multiple times.  In which case all 
   * listeners that have registered will be called again.  If this was constructed with the 
   * expectation of only calling once an {@link IllegalStateException} will be thrown on 
   * subsequent calls. 
   */
  public void callListeners() {
    verifyCanCallListeners();
    
    doCallListeners();
  }
  
  /**
   * Checks to see if listeners can be called (without the need of synchronization).  This will 
   * throw an exception if we were expected to only call once, and that call has already been 
   * invoked.
   * 
   * Assuming this was constructed to only call listeners once, this call sets done to true so 
   * that newly added listeners will be executed immediately.
   */
  protected void verifyCanCallListeners() {
    if (callOnce && ! done.compareAndSet(false, true)) {
      throw new IllegalStateException("Already called listeners");
    }
  }
  
  /**
   * This calls the listeners without any safety checks as to weather it is safe to do so or not.  
   * It is expected that those checks occurred prior to calling this function (either in a 
   * different thread, or at some point earlier to avoid breaking logic around construction with 
   * call listeners once design).
   */
  protected void doCallListeners() {
    synchronized (listenersLock) {
      if (listeners == null) {
        return;
      }
      
      Iterator<Pair<Runnable, Executor>> it = listeners.iterator();
      while (it.hasNext()) {
        Pair<Runnable, Executor> listener = it.next();
        runListener(listener.getLeft(), listener.getRight(), false);
      }
      
      if (callOnce) {
        listeners = null;
      }
    }
  }
  
  /**
   * Invokes a single listener, if an executor is provided that listener is invoked on that 
   * executor, otherwise it runs in this thread.
   * 
   * @param listener Listener to run
   * @param executor Executor to run listener on, or null to run on calling thread
   * @param throwException {@code true} throws exceptions from runnable, {@code false} handles exceptions
   */
  protected void runListener(Runnable listener, Executor executor, 
                             boolean throwException) {
    try {
      if (executor != null) {
        executor.execute(listener);
      } else {
        listener.run();
      }
    } catch (Throwable t) {
      if (throwException) {
        throw ExceptionUtils.makeRuntime(t);
      } else {
        ExceptionUtils.handleException(t);
      }
    }
  }

  /**
   * Adds a listener to be called.  If the {@link RunnableListenerHelper} was constructed with 
   * {@code true} (listeners can only be called once) then this listener will be called 
   * immediately.  This just defers to the other addListener call, providing null for the executor.  
   * So when the listener runs, it will be on the same thread as the one invoking 
   * {@link #callListeners()}.
   * 
   * @param listener runnable to call when trigger event called
   */
  public void addListener(Runnable listener) {
    addListener(listener, null);
  }

  /**
   * Adds a listener to be called.  If the {@link RunnableListenerHelper} was constructed with 
   * {@code true} (listeners can only be called once) then this listener will be called 
   * immediately.  If the executor is null it will be called either on this thread or the thread 
   * calling {@link #callListeners()} (depending on the previous condition).
   * 
   * If an {@link Executor} is provided, and that Executor is NOT single threaded, the listener 
   * may be called concurrently.  You can ensure this wont happen by using the 
   * {@link org.threadly.concurrent.KeyDistributedExecutor} to get an executor from a single key, 
   * or by using the {@link org.threadly.concurrent.limiter.ExecutorLimiter} with a limit of one, 
   * or an instance of the {@link org.threadly.concurrent.SingleThreadScheduler}.
   * 
   * @param listener runnable to call when trigger event called
   * @param executor executor listener should run on, or {@code null}
   */
  public void addListener(Runnable listener, Executor executor) {
    if (listener == null) {
      return;
    }
    
    boolean runListener = done.get();
    if (! runListener) {
      boolean addingFromCallingThread = Thread.holdsLock(listenersLock);
      synchronized (listenersLock) {
        // done should only be set to true if we are only calling listeners once
        if (! (runListener = done.get())) {
          if (addingFromCallingThread) {
            // we must create a new instance of listeners to prevent a ConcurrentModificationException
            // we know at this point that listeners can not be null
            List<Pair<Runnable, Executor>> newListeners = 
                new ArrayList<Pair<Runnable, Executor>>(listeners.size() + 1);
            newListeners.addAll(listeners);
            newListeners.add(new Pair<Runnable, Executor>(listener, executor));
            
            listeners = newListeners;
          } else {
            if (listeners == null) {
              listeners = new ArrayList<Pair<Runnable, Executor>>(2);
            }
            listeners.add(new Pair<Runnable, Executor>(listener, executor));
          }
        }
      }
    }
    
    if (runListener) {
      // run listener outside of lock
      runListener(listener, executor, true);
    }
  }
  
  /**
   * Attempts to remove a listener waiting to be called.
   * 
   * @param listener listener instance to be removed
   * @return {@code true} if the listener was removed
   */
  public boolean removeListener(Runnable listener) {
    boolean removingFromCallingThread = Thread.holdsLock(listenersLock);
    synchronized (listenersLock) {
      if (listeners == null) {
        return false;
      }
      
      if (removingFromCallingThread) {
        listeners = new ArrayList<Pair<Runnable, Executor>>(listeners);
      }
      Iterator<Pair<Runnable, Executor>> it = listeners.iterator();
      while (it.hasNext()) {
        if (ContainerHelper.isContained(it.next().getLeft(), listener)) {
          it.remove();
          return true;
        }
      }
      
      return false;
    }
  }
  
  /**
   * Removes all listeners currently registered. 
   */
  public void clearListeners() {
    synchronized (listenersLock) {
      listeners = null;
    }
  }
  
  /**
   * Returns how many listeners were added, and will be ran on the next call to 
   * {@code callListeners}.  If this was constructed to only run once, all listeners will be 
   * removed after called, and thus this will report zero after callListeners has been invoked.
   * 
   * @return number of listeners registered to be called
   */
  public int registeredListenerCount() {
    synchronized (listenersLock) {
      return listeners == null ? 0 : listeners.size();
    }
  }
}

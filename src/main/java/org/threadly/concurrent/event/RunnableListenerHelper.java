package org.threadly.concurrent.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import org.threadly.concurrent.ContainerHelper;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;

/**
 * Class which assist with holding and calling to Runnable listeners.  In parallel designs it is 
 * common to have things subscribe for actions to occur (to later be alerted once an action 
 * occurs).  This class makes it easy to allow things to register as a listener.
 * <p>
 * For listener designs which are not using runnables, look at {@link ListenerHelper}.  
 * {@link ListenerHelper} allows you to create similar designs while using any any interface to 
 * call back on.
 * 
 * @since 2.2.0 (since 1.1.0 as org.threadly.concurrent.ListenerHelper)
 */
public class RunnableListenerHelper {
  protected final Object listenersLock;
  protected final boolean callOnce;
  protected volatile boolean done;
  protected List<Runnable> inThreadListeners;
  protected List<Pair<Runnable, Executor>> executorListeners;
  
  /**
   * Constructs a new {@link RunnableListenerHelper}.  This can call listeners only once, or every 
   * time {@link #callListeners()} is called.
   * 
   * @param callListenersOnce {@code true} if listeners should only be called once
   */
  public RunnableListenerHelper(boolean callListenersOnce) {
    this.listenersLock = new Object();
    this.callOnce = callListenersOnce;
    this.done = false;
    this.inThreadListeners = null;
    this.executorListeners = null;
  }
  
  /**
   * Return a collection of the currently subscribed listener instances.  This returned collection 
   * can NOT be modified.
   * 
   * @return A non-null collection of currently subscribed listeners
   */
  public Collection<Runnable> getSubscribedListeners() {
    synchronized (listenersLock) {
      if (inThreadListeners == null && executorListeners == null) {
        return Collections.emptyList();
      } else if (inThreadListeners == null) {
        return Collections.unmodifiableList(Pair.collectLeft(executorListeners));
      } else if (executorListeners == null) {
        return Collections.unmodifiableList(inThreadListeners);
      } else {
        List<Runnable> listeners = Pair.collectLeft(executorListeners);
        // dependent on modifiable listeners, unit test verified
        listeners.addAll(inThreadListeners);
        return Collections.unmodifiableList(listeners);
      }
    }
  }
  
  /**
   * Will call all listeners that are registered with this helper.  If any listeners were provided 
   * without an executor, they will execute in the calling thread.  No exceptions will be thrown 
   * in this calling thread if any exceptions occur from the listeners.
   * <p>
   * If calling multiple times, this will only have an effect if constructed with a {@code false}, 
   * indicating that listeners can expect to be called multiple times.  In which case all 
   * listeners that have registered will be called again.  If this was constructed with the 
   * expectation of only calling once an {@link IllegalStateException} will be thrown on 
   * subsequent calls. 
   */
  public void callListeners() {
    synchronized (listenersLock) {
      if (callOnce) {
        if (done) {
          throw new IllegalStateException("Already called listeners");
        } else {
          done = true;
        }
      }
      
      doCallListeners();
    }
  }
  
  /**
   * {@code listenersLock} MUST BE SYNCHRONIZED BEFORE INVOKING THIS.
   * <p>
   * This calls the listeners without any safety checks as to weather it is safe to do so or not.  
   * It is expected that those checks occurred prior to calling this function (either in a 
   * different thread, or at some point earlier to avoid breaking logic around construction with 
   * call listeners once design).
   */
  protected void doCallListeners() {
    if (executorListeners != null) {
      for (Pair<Runnable, Executor> listener : executorListeners) {
        runListener(listener.getLeft(), listener.getRight(), false);
      }
    }
    if (inThreadListeners != null) {
      for (Runnable listener : inThreadListeners) {
        runListener(listener, null, false);
      }
    }
    
    if (callOnce) {
      executorListeners = null;
      inThreadListeners = null;
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
  protected void runListener(Runnable listener, Executor executor, boolean throwException) {
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
    addListener(listener, null, null);
  }

  /**
   * Adds a listener to be called.  If the {@link RunnableListenerHelper} was constructed with 
   * {@code true} (listeners can only be called once) then this listener will be called 
   * immediately.  If the executor is null it will be called either on this thread or the thread 
   * calling {@link #callListeners()} (depending on the previous condition).
   * <p>
   * If an {@link Executor} is provided, and that Executor is NOT single threaded, the listener 
   * may be called concurrently.  You can ensure this wont happen by using the 
   * {@link org.threadly.concurrent.wrapper.KeyDistributedExecutor} to get an executor from a 
   * single key, or by using the {@link org.threadly.concurrent.wrapper.limiter.ExecutorLimiter} 
   * with a limit of one, or an instance of the 
   * {@link org.threadly.concurrent.SingleThreadScheduler}.
   * 
   * @param listener runnable to call when trigger event called
   * @param executor executor listener should run on, or {@code null}
   */
  public void addListener(Runnable listener, Executor executor) {
    addListener(listener, executor, executor);
  }

  /**
   * Adds a listener to be called.  If the {@link RunnableListenerHelper} was constructed with 
   * {@code true} (listeners can only be called once) then this listener will be called 
   * immediately.
   * <p>
   * This allows you to provide different executors to use depending on the state of this 
   * {@link RunnableListenerHelper}.  This is typically used as an optimization inside the threadly 
   * library only.  Most users will be looking for the {@link #addListener(Runnable, Executor)} 
   * which just provides that executor to both arguments here.
   * 
   * @param listener runnable to call when trigger event called
   * @param queueExecutor executor listener should run on if this has to queue, or {@code null}
   * @param inThreadExecutionExecutor executor listener should run on if this helpers state has transitioned to done
   */
  public void addListener(Runnable listener, 
                          Executor queueExecutor, Executor inThreadExecutionExecutor) {
    if (listener == null) {
      return;
    }
    
    boolean runListener = done;
    if (! runListener) {
      boolean addingFromCallingThread = Thread.holdsLock(listenersLock);
      synchronized (listenersLock) {
        // done should only be set to true if we are only calling listeners once
        if (! (runListener = done)) {
          if (queueExecutor != null) {
            if (addingFromCallingThread && executorListeners != null) {
              // copy to prevent a ConcurrentModificationException
              executorListeners = copyAndAdd(executorListeners, new Pair<>(listener, queueExecutor));
            } else {
              if (executorListeners == null) {
                executorListeners = new ArrayList<>(2);
              }
              executorListeners.add(new Pair<>(listener, queueExecutor));
            }
          } else {
            if (addingFromCallingThread && inThreadListeners != null) {
              // copy to prevent a ConcurrentModificationException
              inThreadListeners = copyAndAdd(inThreadListeners, listener);
            } else {
              if (inThreadListeners == null) {
                inThreadListeners = new ArrayList<>(2);
              }
              inThreadListeners.add(listener);
            }
          }
        }
      }
    }
    
    if (runListener) {
      // run listener outside of lock
      runListener(listener, inThreadExecutionExecutor, true);
    }
  }
  
  private static <T> List<T> copyAndAdd(List<T> sourceList, T item) {
    List<T> result = new ArrayList<>(sourceList.size() + 1);
    result.addAll(sourceList);
    result.add(item);
    return result;
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
      if (executorListeners != null) {
        int i = 0;
        for (Pair<Runnable, Executor> p : executorListeners) {
          if (ContainerHelper.isContained(p.getLeft(), listener)) {
            if (removingFromCallingThread) {
              executorListeners = new ArrayList<>(executorListeners);
            }
            executorListeners.remove(i);
            return true;
          } else {
            i++;  // try next index if there is one
          }
        }
      }
      if (inThreadListeners != null) {
        int i = 0;
        for (Runnable r : inThreadListeners) {
          if (ContainerHelper.isContained(r, listener)) {
            if (removingFromCallingThread) {
              inThreadListeners = new ArrayList<>(inThreadListeners);
            }
            inThreadListeners.remove(i);
            return true;
          } else {
            i++;  // try next index if there is one
          }
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
      executorListeners = null;
      inThreadListeners = null;
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
      return (executorListeners == null ? 0 : executorListeners.size()) + 
               (inThreadListeners == null ? 0 : inThreadListeners.size());
    }
  }
}

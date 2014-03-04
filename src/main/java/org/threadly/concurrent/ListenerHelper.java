package org.threadly.concurrent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import org.threadly.util.ExceptionUtils;

/**
 * <p>Class which assist with holding and calling to listeners.  In parallel 
 * designs it is common to have things subscribe for actions to occur (to 
 * be alerted).  This class makes it easy to allow things to register as a 
 * listener.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.1.0
 */
public class ListenerHelper {
  protected final boolean callListenersOnce;
  protected final Map<Runnable, Executor> listeners;
  protected boolean done;
  
  /**
   * Constructs a new {@link ListenerHelper}.  This can call listeners 
   * one time, or every time callListeners is called.
   * 
   * @param callListenersOnce true if listeners should only be called once
   */
  public ListenerHelper(boolean callListenersOnce) {
    this.callListenersOnce = callListenersOnce;
    this.listeners = new HashMap<Runnable, Executor>();
    done = false;
  }
  
  /**
   * Will call all listeners that are registered with this helper.  If any 
   * listeners were provided without an executor, they will execute in the 
   * calling thread.  No exceptions will be thrown in this calling thread if 
   * any exceptions occur from the listeners.
   * 
   * If calling multiple times, this will only have an effect if constructed 
   * with a false, indicating that listeners can expect to be called multiple 
   * times.  In which case all listeners that have registered will be called 
   * again.
   */
  public void callListeners() {
    synchronized (listeners) {
      done = true;
      Iterator<Entry<Runnable, Executor>> it = listeners.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Runnable, Executor> listener = it.next();
        runListener(listener.getKey(), listener.getValue(), false);
      }
      
      if (callListenersOnce) {
        listeners.clear();
      }
    }
  }
  
  protected void runListener(Runnable listener, Executor executor, 
                             boolean throwException) {
    if (executor != null) {
      executor.execute(listener);
    } else {
      try {
        listener.run();
      } catch (RuntimeException e) {
        if (throwException) {
          throw e;
        } else {
          ExceptionUtils.handleException(e);
        }
      }
    }
  }

  /**
   * Adds a listener to be tracked.  If the {@link ListenerHelper} was constructed 
   * with true (listeners can only be called once) then this listener will be called 
   * immediately.  If the executor is null it will be called either on this thread 
   * or the thread calling "callListeners" (depending on the previous condition).
   * 
   * @param listener runnable to call when trigger event called
   * @param executor executor listener should run on, or null
   */
  public void addListener(Runnable listener, Executor executor) {
    if (listener == null) {
      throw new IllegalArgumentException("Can not provide a null listener runnable");
    }
    
    synchronized (listeners) {
      if (callListenersOnce && done) {
        runListener(listener, executor, true);
      } else {
        listeners.put(listener, executor);
      }
    }
  }
  
  /**
   * Call to see how many listeners were added, and will be called on the next 
   * call to "callListeners".
   * 
   * @return number of listeners registered to be called
   */
  public int registeredListenerCount() {
    synchronized (listeners) {
      return listeners.size();
    }
  }
}

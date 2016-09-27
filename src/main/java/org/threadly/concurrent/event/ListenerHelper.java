package org.threadly.concurrent.event;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;

/**
 * <p>Class which assist with holding and calling to listeners of any interface.  In parallel 
 * designs it is common to have things subscribe for actions to occur (to later be alerted once an 
 * action occurs).  This class makes it easy to allow things to register as a listener.</p>
 * 
 * <p>For listener designs which do NOT need to provide arguments for their listeners, look at 
 * using {@link RunnableListenerHelper}.  {@link RunnableListenerHelper} is more efficient and 
 * flexible for listeners of that type.  It also has a cleaner and easier to use interface.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0
 * @param <T> Interface for listeners to implement and called into with
 */
public class ListenerHelper<T> {
  protected final T proxyInstance;
  protected final Object listenersLock;
  protected List<Pair<T, Executor>> listeners;
  
  /**
   * Constructs a new {@link ListenerHelper} that will handle listeners with the provided 
   * interface.  The provided class MUST be an interface.
   * 
   * @param listenerInterface Interface that listeners need to implement
   */
  public ListenerHelper(Class<? super T> listenerInterface) {
    ArgumentVerifier.assertNotNull(listenerInterface, "listenerInterface");
    if (! listenerInterface.isInterface()) {
      throw new IllegalArgumentException("listenerInterface must be an interface");
    }
    
    proxyInstance = makeProxyInstance(listenerInterface);
    listenersLock = new Object();
  }
  
  /**
   * Constructs an instance of the provided interface to be used as the proxy which will end up 
   * calling the stored listeners.  This will only be invoked once during construction time.  This 
   * is designed to allow extending classes to provide their own implementations for how listeners 
   * are called.
   * 
   * @param listenerInterface Interface that listeners need to implement
   * @return Instance of the interface which will call listeners
   */
  @SuppressWarnings("unchecked")
  protected T makeProxyInstance(Class<? super T> listenerInterface) {
    return (T) Proxy.newProxyInstance(listenerInterface.getClassLoader(), 
                                      new Class<?>[] { listenerInterface }, 
                                      new ListenerCaller());
  }
  
  /**
   * Return a collection of the currently subscribed listener instances.  This returned collection 
   * can NOT be modified.
   * 
   * @return A non-null collection of currently subscribed listeners
   */
  public Collection<T> getSubscribedListeners() {
    synchronized (listenersLock) {
      if (listeners == null) {
        return Collections.emptyList();
      } else {
        return Collections.unmodifiableList(Pair.collectLeft(listeners));
      }
    }
  }
  
  /**
   * Calls to notify the subscribed listeners with the given call.  This returns an implementation 
   * of the listener interface, you can then call to the function you wish to have called on the 
   * listeners (of course providing the arguments you want the listeners to be called with).
   * 
   * Any calls off the returned instance will execute on all subscribed listeners.  If those 
   * listeners were provided with an executor the execution for calling that listener will happen 
   * on the provided executor.  If no executor was provided, the execution of the listener will 
   * happen on the thread invoking this call.
   * 
   * @return Implementation of listener interface to have call subscribed listeners
   */
  public T call() {
    return proxyInstance;
  }
  
  /**
   * Adds a listener to be executed on the next {@link #call()} to this instance.  This is the same 
   * as adding a listener and providing null for the {@link Executor}.
   *  
   * @param listener Listener to be called when {@link #call()} is invoked
   */
  public void addListener(T listener) {
    addListener(listener, null);
  }
  
  /**
   * Adds a listener to be executed on the next {@link #call()} to this instance.  If an executor 
   * is provided, on the next {@link #call()} a task will be put on the executor to call this 
   * listener.  If none is provided, the listener will be executed on the thread that is invoking 
   * the {@link #call()}.
   * 
   * If an {@link Executor} is provided, and that {@link Executor} is NOT single threaded, the 
   * listener may be called concurrently.  You can ensure this wont happen by using the 
   * {@link org.threadly.concurrent.wrapper.KeyDistributedExecutor} to get an executor from a 
   * single key, or by using the {@link org.threadly.concurrent.wrapper.limiter.ExecutorLimiter} 
   * with a limit of one, or an instance of the 
   * {@link org.threadly.concurrent.SingleThreadScheduler}.
   * 
   * @param listener Listener to be called when {@link #call()} is invoked
   * @param executor {@link Executor} to call listener on, or {@code null}
   */
  public void addListener(T listener, Executor executor) {
    if (listener == null) {
      return;
    }
    
    boolean addingFromCallingThread = Thread.holdsLock(listenersLock);
    synchronized (listenersLock) {
      if (addingFromCallingThread) {
        // we must create a new instance of listeners to prevent a ConcurrentModificationException
        // we know at this point that listeners can not be null
        List<Pair<T, Executor>> newListeners = 
            new ArrayList<Pair<T, Executor>>(listeners.size() + 1);
        newListeners.addAll(listeners);
        newListeners.add(new Pair<T, Executor>(listener, executor));
        
        listeners = newListeners;
      } else {
        if (listeners == null) {
          listeners = new ArrayList<Pair<T, Executor>>(2);
        }
        listeners.add(new Pair<T, Executor>(listener, executor));
      }
    }
  }

  /**
   * Attempts to remove a listener waiting to be called.
   * 
   * @param listener listener instance to be removed
   * @return {@code true} if the listener was removed
   */
  public boolean removeListener(T listener) {
    boolean removingFromCallingThread = Thread.holdsLock(listenersLock);
    synchronized (listenersLock) {
      if (listeners == null) {
        return false;
      }
      
      if (removingFromCallingThread) {
        listeners = new ArrayList<Pair<T, Executor>>(listeners);
      }
      Iterator<Pair<T, Executor>> it = listeners.iterator();
      while (it.hasNext()) {
        if (it.next().getLeft().equals(listener)) {
          it.remove();
          return true;
        }
      }
      
      return false;
    }
  }
  
  /**
   * Removes all listener currently registered. 
   */
  public void clearListeners() {
    synchronized (listenersLock) {
      listeners = null;
    }
  }
  
  /**
   * Returns how many listeners were added, and will be ran on the next {@link #call()} invocation.
   * 
   * @return number of listeners registered to be called
   */
  public int registeredListenerCount() {
    synchronized (listenersLock) {
      return listeners == null ? 0 : listeners.size();
    }
  }
  
  /**
   * <p>Implementation of {@link InvocationHandler} that calls the provided listeners when the 
   * invocation occurs.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.2.0
   */
  protected class ListenerCaller implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      verifyValidMethod(method);
      
      callListeners(method, args);
      
      // always returns null
      return null;
    }
    
    /**
     * Verifies that the method can be a valid invocation on the interface.
     * 
     * @param method Method to check against for invocation
     */
    protected void verifyValidMethod(Method method) {
      if (! method.getReturnType().equals(Void.TYPE)) {
        throw new RuntimeException("Can only call listeners with a void return type");
      }
    }
    
    /**
     * Calls all registered listeners for a given method with the given arguments.
     * 
     * @param method Method to invoke
     * @param args Arguments to provide to each listener's invocation
     */
    protected void callListeners(final Method method, final Object[] args) {
      synchronized (listenersLock) {
        if (listeners != null) {
          Iterator<Pair<T, Executor>> it = listeners.iterator();
          while (it.hasNext()) {
            final Pair<T, Executor> listener = it.next();
            if (listener.getRight() != null) {
              listener.getRight().execute(new Runnable() {
                @Override
                public void run() {
                  callListener(listener.getLeft(), method, args);
                }
              });
            } else {
              callListener(listener.getLeft(), method, args);
            }
          }
        }
      }
    }
    
    /**
     * Calls an individual listener, and handles any exceptions which may have resulted.
     * 
     * @param listener Listener instance to invoke
     * @param method Method to invoke
     * @param args Arguments to provide to listeners invocation
     */
    protected void callListener(T listener, Method method, Object[] args) {
      try {
        method.invoke(listener, args);
      } catch (IllegalAccessException e) {
        /* should not be possible since only interfaces are allowed, and 
         * all functions in interfaces are public
         */
        ExceptionUtils.handleException(e);
      } catch (InvocationTargetException e) {
        ExceptionUtils.handleException(e.getCause());
      }
    }
  }
}

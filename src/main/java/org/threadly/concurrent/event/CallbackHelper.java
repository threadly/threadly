package org.threadly.concurrent.event;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import org.threadly.util.ExceptionUtils;

/**
 * <p>Class which assist with holding and calling to callbacks.  In parallel designs 
 * it is common to have things subscribe for actions to occur (to later be alerted 
 * once an action occurs).  This class makes it easy to allow things to register as 
 * a listener.</p>
 * 
 * <p>For listener designs which do NOT need to provide arguments for their callbacks, 
 * look at using {@link ListenerHelper}.  {@link ListenerHelper} is more efficient and 
 * flexible for listeners of that type.  It also has a cleaner and easier to use 
 * interface.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0
 * @param <T> Interface for callbacks to be used
 */
public class CallbackHelper<T> {
  /**
   * This static function allows for quick and easy construction of the 
   * {@link CallbackHelper}.  It is equivalent to the normal constructor, but 
   * requires less code to do that construction.
   * 
   * @param callbackInterface Interface that callbacks need to implement
   * @return New instance of the {@link CallbackHelper}
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> CallbackHelper<T> build(Class<T> callbackInterface) {
    return new CallbackHelper(callbackInterface);
  }
  
  protected final T proxyInstance;
  protected final Object callbacksLock;
  protected Map<T, Executor> callbacks;
  
  /**
   * Constructs a new {@link CallbackHelper} that will handle callbacks with 
   * the provided interface.  The provided class MUST be an interface.
   * 
   * @param callbackInterface Interface that callbacks need to implement
   */
  @SuppressWarnings("unchecked")
  public CallbackHelper(Class<T> callbackInterface) {
    if (callbackInterface == null) {
      throw new IllegalArgumentException("Must provide interface for callbacks");
    } else if (! callbackInterface.isInterface()) {
      throw new IllegalArgumentException("callbackInterface must be an interface");
    }
    
    proxyInstance = (T) Proxy.newProxyInstance(callbackInterface.getClassLoader(), 
                                               new Class[] { callbackInterface }, 
                                               new CallbackCaller());
    callbacksLock = new Object();
  }
  
  /**
   * Calls to notify the subscribed callbacks with the given call.  This returns an 
   * implementation of the callback interface, you can then call to the function you 
   * wish to have called on the listeners (of course providing the arguments you want 
   * the listeners to be called with).
   * 
   * @return Implementation of callback interface to have call subscribed callbacks
   */
  public T call() {
    return proxyInstance;
  }
  
  /**
   *  Adds a callback to be executed on the next .call() to this instance.
   *  
   * @param callback Callback to be called when .call() occurs
   */
  public void addCallback(T callback) {
    addCallback(callback, null);
  }
  
  /**
   * Adds a callback to be executed on the next .call() to this instance.  If an 
   * executor is provided, on the next .call() a task will be put on the executor 
   * to call this callback.  If none is provided, the callback will be executed 
   * on the thread that is invoking the .call().
   * 
   * @param callback Callback to be called when .call() occurs
   * @param executor Executor to call callback on, or null
   */
  public void addCallback(T callback, Executor executor) {
    if (callback == null) {
      throw new IllegalArgumentException("Can not provide a null callback");
    }
    
    boolean addingFromCallingThread = Thread.holdsLock(callbacksLock);
    synchronized (callbacksLock) {
      if (addingFromCallingThread) {
        // we must create a new instance of listeners to prevent a ConcurrentModificationException
        // we know at this point that listeners can not be null
        Map<T, Executor> newCallbacks = new HashMap<T, Executor>(callbacks.size() + 1);
        newCallbacks.putAll(callbacks);
        newCallbacks.put(callback, executor);
        
        callbacks = newCallbacks;
      } else {
        if (callbacks == null) {
          callbacks = new HashMap<T, Executor>();
        }
        callbacks.put(callback, executor);
      }
    }
  }

  /**
   * Attempts to remove a callback waiting to be called.
   * 
   * @param callback callback instance to be removed
   * @return true if the callback was removed
   */
  public boolean removeCallback(T callback) {
    boolean removingFromCallingThread = Thread.holdsLock(callbacksLock);
    synchronized (callbacksLock) {
      if (callbacks == null) {
        return false;
      } else if (callbacks.containsKey(callback)) {
        if (removingFromCallingThread) {
          callbacks = new HashMap<T, Executor>(callbacks);
        }
        callbacks.remove(callback);
        return true;
      } else {
        return false;
      }
    }
  }
  
  /**
   * Removes all callback currently registered. 
   */
  public void clearCallbacks() {
    synchronized (callbacksLock) {
      callbacks = null;
    }
  }
  
  /**
   * Returns how many callbacks were added, and will be ran on the next 
   * call.
   * 
   * @return number of callbacks registered to be called
   */
  public int registeredCallbackCount() {
    synchronized (callbacksLock) {
      return callbacks == null ? 0 : callbacks.size();
    }
  }
  
  /**
   * <p>Implementation of {@link InvocationHandler} that calls the provided 
   * listeners when the invocation occurs.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class CallbackCaller implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) {
      if (! method.getReturnType().equals(Void.TYPE)) {
        throw new RuntimeException("Can only call listeners with a void return type");
      }
      
      synchronized (callbacksLock) {
        if (callbacks != null) {
          Iterator<Entry<T, Executor>> it = callbacks.entrySet().iterator();
          while (it.hasNext()) {
            final Entry<T, Executor> callback = it.next();
            if (callback.getValue() != null) {
              callback.getValue().execute(new Runnable() {
                @Override
                public void run() {
                  callCallback(callback.getKey(), method, args);
                }
              });
            } else {
              callCallback(callback.getKey(), method, args);
            }
          }
        }
      }
      
      // always returns null
      return null;
    }
    
    protected void callCallback(T callback, Method method, Object[] args) {
      try {
        method.invoke(callback, args);
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

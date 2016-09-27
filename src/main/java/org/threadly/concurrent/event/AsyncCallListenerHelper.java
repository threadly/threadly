package org.threadly.concurrent.event;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Executor;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>This class changes the behavior of how listeners are called from the parent class 
 * {@link ListenerHelper}.  In this implementation when listeners are invoked with the 
 * {@link #call()} function, the invocation of all the listeners will occur on the 
 * {@link Executor} that was provided at construction.  If the listener was added without a 
 * provided executor it will then run on the provided executor (in the thread doing the 
 * {@link #call()} invocation, AKA it will run that listener before executing other listeners).  
 * If the listener was added with a provided executor, that listener will still execute on the 
 * provided executor (so not necessarily the executor provided at construction time).</p>
 * 
 * <p>If it is desired that all listeners are executed asynchronously from each other, you should 
 * actually use the normal {@link ListenerHelper}, and instead just ensure that an executor is 
 * provided when each listener is added.  If you want listeners to execute concurrently from each 
 * other, but not concurrently for any single listener, {@link DefaultExecutorListenerHelper} is 
 * likely a better choice.  This class is only designed to ensure that {@link #call()} invocations 
 * will never block.</p>
 * 
 * <p>To better clarify when this implementation makes sense compared to {@link ListenerHelper} 
 * and {@link DefaultExecutorListenerHelper}.  If you have a LOT of quick running listeners, this 
 * is the right class for you.  If you have few listeners that execute quickly, then the normal 
 * {@link ListenerHelper} is likely a better choice.  If you have long running/complex listeners, 
 * {@link DefaultExecutorListenerHelper} is possibly the better choice.  Alternative for the last 
 * condition you could use the normal {@link ListenerHelper}, and just ensure that an executor is 
 * provided for every listener (but if you want to ensure a given listener is not executed 
 * concurrently the {@link DefaultExecutorListenerHelper} will handle this for you).</p>
 * 
 * <p>It is important to note that this class does not ensure ordering of how listeners are 
 * called.  For example if you provided a multi-threaded executor, and are calling the listeners 
 * twice, those listeners call order is non-deterministic.  If this is important to you, you must 
 * ensure that the Executor provided is single threaded (ie by using the 
 * {@link org.threadly.concurrent.wrapper.KeyDistributedExecutor} to get an executor from a single 
 * key, or by using the {@link org.threadly.concurrent.wrapper.limiter.ExecutorLimiter} with a 
 * limit of one, or an instance of the {@link org.threadly.concurrent.SingleThreadScheduler}).</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0
 * @param <T> Interface for listeners to implement and called into with
 */
public class AsyncCallListenerHelper<T> extends ListenerHelper<T> {
  protected final Executor executor;

  /**
   * Constructs a new {@link AsyncCallListenerHelper} that will handle listeners with the provided 
   * interface.  The provided class MUST be an interface.  All {@link #call()} invocations will 
   * occur on the provided executor, but listeners may run on different executors if they are 
   * added with their respective executors.
   * 
   * @param listenerInterface Interface that listeners need to implement
   * @param executor {@link Executor} that {@link #call()} invocation will occur on
   */
  public AsyncCallListenerHelper(Class<? super T> listenerInterface, Executor executor) {
    super(listenerInterface);

    ArgumentVerifier.assertNotNull(executor, "executor");
    
    this.executor = executor;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected T makeProxyInstance(Class<? super T> listenerInterface) {
    return (T) Proxy.newProxyInstance(listenerInterface.getClassLoader(), 
                                      new Class<?>[] { listenerInterface }, 
                                      new AsyncListenerCaller());
  }
  
  /**
   * Implementation of the {@link ListenerCaller} which verifies the method, and then calls the 
   * listeners on the {@link Executor} that is stored within the {@link AsyncCallListenerHelper} 
   * class.
   * 
   * @author jent - Mike Jensen
   * @since 2.2.0
   */
  protected class AsyncListenerCaller extends ListenerCaller {
    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) {
      verifyValidMethod(method);
      
      executor.execute(new Runnable() {
        @Override
        public void run() {
          callListeners(method, args);
        }
      });
      
      // always returns null
      return null;
    }
  }
}

package org.threadly.concurrent.event;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Executor;

import org.threadly.util.ExceptionUtils;

/**
 * Simple utility for multiplying invocations across multiple instances of a given interface.
 * 
 * @since 4.3.0
 */
@SuppressWarnings("unchecked")
public class InvocationTee {
  private InvocationTee() {
    // utility class
  }

  /**
   * This creates a tee proxy for a given class with a set of instances of said interface.  Any 
   * invocation to the returned instance (proxy), will be multiple to all provided instances.  If 
   * provided arguments are mutable, and an instance mutates that argument, future invoked 
   * instances may see that modification.  It is not deterministic which order the instances will 
   * be invoked in.  
   * <p>
   * If any listeners throw an exception it will be delegated to 
   * {@link org.threadly.util.ExceptionUtils#handleException(Throwable)}.  So a listener throwing 
   * an exception will NOT interrupt other listeners from being invoked.  If you want you can 
   * handle thrown exceptions by setting an {@link org.threadly.util.ExceptionHandler} into 
   * {@link org.threadly.util.ExceptionUtils}.  Make sure the handler is set it in a way that makes 
   * sense based off what thread will be invoking the returned interface.  
   * <p>
   * Under the hood this depends on {@link ListenerHelper}, and thus has the same limitations.  
   * Most specifically the provided {@code teeInterface} must in fact be an interface, and not a 
   * an abstract class, or other non-interface types.  In addition any invocations called to this 
   * must be a {@code void} return type.
   * 
   * @param <T> Type representing interface to multiple invocations of
   * @param teeInterface Interface class for which the returned instance must implement
   * @param instances Instances of said interface which invocations should be multiplied to
   * @return A returned interface which will map all invocations to all provided interfaces
   */
  public static <T> T tee(Class<? super T> teeInterface, T ... instances) {
    return setupHelper(new ListenerHelper<>(teeInterface), instances);
  }
  
  /**
   * This implementation is variation from {@link #tee(Class, Object...)}, read that documentation 
   * first.  
   * <p>
   * The behavior modifies from that implementation is in how exceptions are handled.  Rather than 
   * handling them and ensuring that all instances are invoked, this will throw the exception.  
   * More specifically when an instance throws an exception it will prevent further instances from 
   * bring invoked, and that exception will bubble up to the original interface invoker.
   * 
   * @param <T> Type representing interface to multiple invocations of
   * @param teeInterface Interface class for which the returned instance must implement
   * @param instances Instances of said interface which invocations should be multiplied to
   * @return A returned interface which will map all invocations to all provided interfaces
   */
  public static <T> T teeWithExceptionThrowing(Class<? super T> teeInterface, T ... instances) {
    ListenerHelper<T> lh = new ListenerHelper<T>(teeInterface) {
      @Override
      protected T makeProxyInstance(Class<? super T> listenerInterface) {
        return (T) Proxy.newProxyInstance(listenerInterface.getClassLoader(), 
                                          new Class<?>[] { listenerInterface }, 
                                          new ListenerCaller() {
          @Override
          protected void callListener(T listener, Method method, Object[] args) {
            try {
              method.invoke(listener, args);
            } catch (IllegalAccessException e) {
              /* should not be possible since only interfaces are allowed, and 
               * all functions in interfaces are public
               */
              ExceptionUtils.handleException(e);
            } catch (InvocationTargetException e) {
              // throw exception to interrupt calling handlers
              throw ExceptionUtils.makeRuntime(e.getCause());
            }
          }
        });
      }
    };
    
    return setupHelper(lh, instances);
  }
  
  /**
   * This implementation is variation from {@link #tee(Class, Object...)}, read that documentation 
   * first.  
   * <p>
   * The behavior modifies from that implementation is that all instances will be called by a 
   * executor.  This means that invocations into the returned instance will return immediately, 
   * but instance invocation will happen async.  It is guaranteed that any single instance will NOT 
   * be invoked in parallel.  Thus meaning that if you invoke into the proxy two times before 
   * a give instance finishes processing the first invocation, the second invocation will queue and 
   * only be invoked after the first finishes.  
   * <p>
   * Under the hood {@link DefaultExecutorListenerHelper} is used to provide this behavior.
   * 
   * @param <T> Type representing interface to multiple invocations of
   * @param executor Executor that instances will be invoked on to
   * @param teeInterface Interface class for which the returned instance must implement
   * @param instances Instances of said interface which invocations should be multiplied to
   * @return A returned interface which will map all invocations to all provided interfaces
   */
  public static <T> T teeWithExecutor(Executor executor, 
                                      Class<? super T> teeInterface, T ... instances) {
    return setupHelper(new DefaultExecutorListenerHelper<>(teeInterface, executor), instances);
  }

  private static <T> T setupHelper(ListenerHelper<T> lh, T ... instances) {
    for (T instance : instances) {
      lh.addListener(instance);
    }
    
    return lh.call();
  }
}

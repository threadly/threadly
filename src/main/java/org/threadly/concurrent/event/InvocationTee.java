package org.threadly.concurrent.event;

/**
 * <p>Simple utility for multiplying invocations across multiple instances of a given interface.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
public class InvocationTee {
  /**
   * This creates a tee proxy for a given class with a set of instances of said interface.  Any 
   * invocation to the returned instance (proxy), will be multiple to all provided instances.  If 
   * provided arguments are mutable, and an instance mutates that argument, future invoked 
   * instances may see that modification.  It is not deterministic which order the instances will 
   * be invoked in.  
   * 
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
  @SuppressWarnings("unchecked")
  public static <T> T tee(Class<? super T> teeInterface, T ... instances) {
    ListenerHelper<T> lh = ListenerHelper.build(teeInterface);
    
    for (T instance : instances) {
      lh.addListener(instance);
    }
    
    return lh.call();
  }
}

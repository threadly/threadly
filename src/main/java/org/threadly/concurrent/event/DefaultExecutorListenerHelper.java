package org.threadly.concurrent.event;

import java.util.concurrent.Executor;

import org.threadly.concurrent.wrapper.KeyDistributedExecutor;

/**
 * <p>This class ensures that listener execution will never happen on the thread that invokes 
 * {@link #call()}.  It does this in a different way from how the {@link AsyncCallListenerHelper} 
 * does it.  In this implementation the iteration of the listeners still occurs on the thread 
 * executing the {@link #call()}, but as listeners are added, it is ensured that they are provided 
 * an executor to execute on (so listener execution will actually happen on the executor).  If a 
 * listener is provided with an executor, that provided Executor will NOT be overridden, and 
 * instead it will be used for the listeners execution.</p>
 * 
 * <p>Internally this class uses the {@link KeyDistributedExecutor}, using the listener as the 
 * execution key, to ensure that any single listener will NEVER execute concurrently with 
 * itself.</p>
 * 
 * <p>In general, this implementation is most efficient when there are few listeners, but the 
 * listeners are high complexity, or take a long time to execute.  If you have few listeners AND 
 * they execute quickly, the normal {@link ListenerHelper} is likely a better choice.  If you have 
 * MANY listeners, but they execute very quickly, {@link AsyncCallListenerHelper} is possibly a 
 * better choice.</p>
 * 
 * <p>Unlike {@link AsyncCallListenerHelper}, even if the executor provided here is 
 * multi-threaded, order of listener call's are preserved.  So there is no need to provide a 
 * single threaded executor into this class.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0
 * @param <T> Interface for listeners to implement and called into with
 */
public class DefaultExecutorListenerHelper<T> extends ListenerHelper<T> {
  protected final KeyDistributedExecutor taskDistributor;

  /**
   * Constructs a new {@link DefaultExecutorListenerHelper} that will handle listeners with the 
   * provided interface.  The provided class MUST be an interface.  If any listeners are not 
   * provided an executor, they will execute on the provided executor.
   * 
   * @param listenerInterface Interface that listeners need to implement
   * @param executor Executor to execute listeners which were not provided one by default
   */
  public DefaultExecutorListenerHelper(Class<? super T> listenerInterface, Executor executor) {
    super(listenerInterface);
    
    taskDistributor = new KeyDistributedExecutor(executor);
  }
  
  @Override
  public void addListener(T listener, Executor executor) {
    if (listener == null) {
      return;
    }
    if (executor == null) {
      executor = taskDistributor.getExecutorForKey(listener);
    }
    
    super.addListener(listener, executor);
  }
}

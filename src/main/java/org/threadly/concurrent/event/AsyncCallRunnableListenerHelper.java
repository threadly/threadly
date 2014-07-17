package org.threadly.concurrent.event;

import java.util.concurrent.Executor;

/**
 * <p>This class changes the behavior of how listeners are called from the parent class 
 * {@link RunnableListenerHelper}.  In this implementation when listeners are invoked 
 * with the .callListeners() function, the invocation of all the listeners will occur on the 
 * {@link Executor} that was provided at construction.  If the listener was added without 
 * a provided executor it will then run on the provided executor (in the thread doing the 
 * .callListeners() invocation, aka it will run that listener before executing other 
 * listeners).  If the listener was added with a provided executor, that listener will still 
 * execute on the provided executor (so not necessarily the executor provided at 
 * construction time).<p>
 * 
 * <p>If it is desired that all listeners are executed asynchronously from each other, you 
 * should actually use the normal {@link RunnableListenerHelper}, and instead just ensure 
 * that an executor is provided when each listener is added.  If you want listeners to 
 * execute concurrently from each other, but not concurrently for any single listener, 
 * {@link DefaultExecutorRunnableListenerHelper} is likely a better choice.  This class is 
 * only designed to ensure that .call() invocations will never block.</p>
 * 
 * <p>To better clarify when this implementation makes sense compared to 
 * {@link RunnableListenerHelper} and {@link DefaultExecutorRunnableListenerHelper}.  If you 
 * have a LOT of quick running listeners, this is the right class for you.  If you have few 
 * listeners that execute quickly, then the normal {@link RunnableListenerHelper} is likely 
 * a better choice.  If you have long running/complex listeners, 
 * {@link DefaultExecutorRunnableListenerHelper} is possibly the better choice.  Alternative 
 * for the last condition you could use the normal {@link RunnableListenerHelper}, and just 
 * ensure that an executor is provided for every listener (but if you want to ensure a given 
 * listener is not executed concurrently the {@link DefaultExecutorRunnableListenerHelper} 
 * will handle this for you).</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0
 */
public class AsyncCallRunnableListenerHelper extends RunnableListenerHelper {
  protected final Executor executor;

  /**
   * Constructs a new {@link AsyncCallRunnableListenerHelper}.  This can call listeners 
   * one time, or every time callListeners is called.
   * 
   * @param callListenersOnce true if listeners should only be called once
   * @param executor Executor that callListeners should execute on
   */
  public AsyncCallRunnableListenerHelper(boolean callListenersOnce, Executor executor) {
    super(callListenersOnce);
    
    if (executor == null) {
      throw new IllegalArgumentException("Must provide executor");
    }
    
    this.executor = executor;
  }
  
  @Override
  public void callListeners() {
    verifyCanCallListeners();
      
    executor.execute(new Runnable() {
      @Override
      public void run() {
        AsyncCallRunnableListenerHelper.super.doCallListeners();
      }
    });
  }
}

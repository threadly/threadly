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
 * <p>If it is desired that ALL listeners are executed asynchronously from each other, you 
 * should actually use the normal {@link RunnableListenerHelper}, and instead just ensure 
 * that an executor is provided when each listener is added.  This class is only designed to 
 * ensure that .call() invocations will never block.</p>
 * 
 * <p>To better clarify when this implementation makes sense compared to 
 * {@link RunnableListenerHelper}, If you have a LOT of quick running listeners, this is 
 * the right class for you.  If you have long running/complex listeners, 
 * {@link RunnableListenerHelper} is the right choice, just ensure that an executor is 
 * provided when each listener is added.</p>
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
      throw new IllegalArgumentException("Must provide executor for .call() invocation to occur on");
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

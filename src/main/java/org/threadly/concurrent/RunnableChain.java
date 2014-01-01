package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;

import org.threadly.util.ExceptionUtils;

/**
 * <p>A class to chain multiple runnables to later be run together, 
 * within the same thread.</p>
 * 
 * @author jent - Mike Jensen
 */
public class RunnableChain implements Runnable {
  private final boolean exceptionStopsChain;
  private final Iterable<? extends Runnable> toRun;
  
  /**
   * Constructs a runnable chain with a provided list of runnables to iterate over.
   * 
   * @param exceptionStopsChain true for uncaught exception stops the execution of the chain
   * @param toRun Iterable collection of runnables to run
   */
  public RunnableChain(boolean exceptionStopsChain, 
                       Iterable<? extends Runnable> toRun) {
    if (toRun == null) {
      toRun = new ArrayList<Runnable>(0);
    }
    
    this.exceptionStopsChain = exceptionStopsChain;
    this.toRun = toRun;
  }

  @Override
  public void run() {
    if (exceptionStopsChain) {
      runExceptionsCascade();
    } else {
      runIsolated();
    }
  }
  
  protected void runExceptionsCascade() {
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      it.next().run();
    }
  }
  
  protected void runIsolated() {
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      try {
        it.next().run();
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
      }
    }
  }
}

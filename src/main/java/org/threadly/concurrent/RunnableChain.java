package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;

import org.threadly.util.ExceptionUtils;

/**
 * <p>A class to chain multiple runnables and thus run them all
 * in the same thread.</p>
 * 
 * @author jent - Mike Jensen
 */
public class RunnableChain extends VirtualRunnable {
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
      runRunnable(it.next());
    }
  }
  
  protected void runIsolated() {
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      try {
        runRunnable(it.next());
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
      }
    }
  }
  
  protected void runRunnable(Runnable toRun) {
    if (factory != null && toRun instanceof VirtualRunnable) {
      ((VirtualRunnable)toRun).run(factory);
    } else {
      toRun.run();
    }
  }
}

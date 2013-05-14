package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A class to chain multiple runnables and thus run them all
 * in the same thread.
 * 
 * @author jent - Mike Jensen
 */
public class RunnableChain implements Runnable {
  private final boolean exceptionStopsChain;
  private final List<? extends Runnable> toRun;
  
  /**
   * Constructs a runnable chain with a provided list of runnables to iterate over.
   * 
   * @param exceptionStopsChain true for uncaught exception stops the execution of the chain
   * @param toRun List of runnables to call
   */
  public RunnableChain(boolean exceptionStopsChain, 
                       List<? extends Runnable> toRun) {
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
  
  private void runExceptionsCascade() {
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      it.next().run();
    }
  }
  
  private void runIsolated() {
    Throwable toThrow = null;
    Iterator<? extends Runnable> it = toRun.iterator();
    while (it.hasNext()) {
      try {
        it.next().run();
      } catch (Throwable t) {
        toThrow = t;
      }
    }
    
    if (toThrow != null) {
      throw new RuntimeException("Throwable from runnable chain", toThrow);
    }
  }
}

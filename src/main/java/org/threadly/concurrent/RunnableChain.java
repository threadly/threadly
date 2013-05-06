package org.threadly.concurrent;

import java.util.Iterator;
import java.util.List;

public class RunnableChain implements Runnable {
  private final boolean exceptionStopsChain;
  private final List<? extends Runnable> toRun;
  
  public RunnableChain(boolean exceptionStopsChain, List<? extends Runnable> toRun) {
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

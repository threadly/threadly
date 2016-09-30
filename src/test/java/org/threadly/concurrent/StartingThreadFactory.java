package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadFactory;

/**
 * <p>A bad thread factory, which starts the threads it returns.</p>
 * 
 * @author jent - Mike Jensen
 */
public class StartingThreadFactory implements ThreadFactory {
  private final ConfigurableThreadFactory parentFactory = new ConfigurableThreadFactory();
  private final List<Thread> toKillThreads = new ArrayList<>(2);
  
  @Override
  public Thread newThread(final Runnable r) {
    Runnable livingRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          r.run();
        } finally {
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (InterruptedException e) {
            // let thread exit
            return;
          }
        }
      }
    };
    
    Thread result = parentFactory.newThread(livingRunnable);
    synchronized (toKillThreads) {
      toKillThreads.add(result);
    }
    result.start();
    
    return result;
  }
  
  @SuppressWarnings("javadoc")
  public void killThreads() {
    synchronized (toKillThreads) {
      Iterator<Thread> it = toKillThreads.iterator();
      while (it.hasNext()) {
        it.next().interrupt();
      }
    }
  }
}

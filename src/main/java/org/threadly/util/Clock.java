package org.threadly.util;

import java.util.concurrent.locks.LockSupport;

/**
 * This is a utility class for low-resolution timing which avoids
 * frequent System.currentTimeMillis() calls (which perform poorly 
 * because they require system calls).
 *
 * Each call to Clock.lastKnownTimeMillis() will return the value of
 * System.currentTimeMillis() as of the last call to Clock.accurateTime().
 * This means lastKnownTimeMillis() will only be as accurate as the
 * frequency with which accurateTime() is called.
 * 
 * @author jent - Mike Jensen
 */
public class Clock {
  protected static final boolean UPDATE_CLOCK_AUTOMATICALLY = true;
  protected static final short AUTOMATIC_UPDATE_FREQUENCY_IN_MS = 100;
  protected static final short STOP_PARK_TIME_IN_NANOS = 10000;
  
  private static final Object UPDATE_LOCK = new Object();
  private static volatile long now = System.currentTimeMillis();
  private static ClockUpdater clockUpdater = null;
  
  static {
    if (UPDATE_CLOCK_AUTOMATICALLY) {
      startClockUpdateThread();
    }
  }
  
  private Clock() {
    // don't construct
  }
  
  /** 
   * Starts the clock updating automatically (used for testing).
   */
  public static void startClockUpdateThread() {
    synchronized (UPDATE_LOCK) {
      if (clockUpdater != null) {
        return;
      } else {
        clockUpdater = new ClockUpdater();
        
        Thread thread = new Thread(clockUpdater);
        
        thread.setName("Threadly clock updater");
        thread.setDaemon(true);
        thread.start();
      }
    }
  }

  /**
   * Stops the clock from updating automatically (used for testing).
   * 
   * This call blocks until the automatic update thread stops, or 
   * until this thread is interrupted.
   */
  public static void stopClockUpdateThread() {
    ClockUpdater oldUpdater;
    synchronized (UPDATE_LOCK) {
      oldUpdater = clockUpdater;
      
      clockUpdater = null;
      
      UPDATE_LOCK.notifyAll();
    }
    
    if (oldUpdater != null) {
      Thread currentThread = Thread.currentThread();
      while (! oldUpdater.runnableFinished && 
             ! currentThread.isInterrupted()) {
        LockSupport.parkNanos(STOP_PARK_TIME_IN_NANOS);
      }
    }
  }

  /**
   * Getter for the last known time in milliseconds.  This time is considered semi-accurate, 
   * based off the last time accurate time has been requested, or this class has automatically 
   * updated the time (unless requested to stop automatically updating).
   * 
   * @return last known time in milliseconds
   */
  public static long lastKnownTimeMillis() {
    return now;
  }

  /**
   * Updates the clock and returns the accurate time.
   * 
   * @return accurate time in milliseconds
   */
  public static long accurateTime() {
    return now = System.currentTimeMillis();
  }
  
  /**
   * Runnable which will regularly update the stored clock time.  
   * This runnable is designed to run in it's own dedicated thread.
   * 
   * @author jent - Mike Jensen
   */
  private static class ClockUpdater implements Runnable {
    private volatile boolean runnableFinished = false;
    
    @Override
    public void run() {
      try {
        synchronized (UPDATE_LOCK) {
          while (clockUpdater == this) {
            try {
              accurateTime();
              
              UPDATE_LOCK.wait(AUTOMATIC_UPDATE_FREQUENCY_IN_MS);
            } catch (InterruptedException e) {
              clockUpdater = null;  // let thread exit
            }
          }
        }
      } finally {
        runnableFinished = true;
      }
    }
  }
}

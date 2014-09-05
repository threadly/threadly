package org.threadly.util;

import java.util.concurrent.locks.LockSupport;

/**
 * <p>This is a utility class for low-resolution timing which avoids frequent 
 * System.currentTimeMillis() calls (which perform poorly because they require system calls).</p>
 *
 * <p>Each call to Clock.lastKnownTimeMillis() will return the value of System.currentTimeMillis() 
 * as of the last call to Clock.accurateTime().  This means {@link #lastKnownTimeMillis()} will 
 * only be as accurate as the frequency with which {@link #accurateTimeMillis()} is called.</p>
 * 
 * <p>In order to ensure a minimum level of accuracy, by default a thread is started to call 
 * {@link #accurateTimeMillis()} every 100 milliseconds.  This can be disabled by calling 
 * {@link #stopClockUpdateThread()}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class Clock {
  protected static final boolean UPDATE_CLOCK_AUTOMATICALLY = true;
  protected static final short AUTOMATIC_UPDATE_FREQUENCY_IN_MS = 100;
  protected static final short STOP_PARK_TIME_IN_NANOS = 25000;
  
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
   * Starts a thread to regularly updated the clock automatically.
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
   * Stops the clock from updating automatically.  
   * 
   * This call blocks until the automatic update thread stops, or until this thread is interrupted.
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
   * Getter for the last known time in milliseconds.  This time is considered semi-accurate, based 
   * off the last time accurate time has been requested, or this class has automatically updated 
   * the time (unless requested to stop automatically updating).
   * 
   * @return last known time in milliseconds
   */
  public static long lastKnownTimeMillis() {
    return now;
  }

  /**
   * Updates the clock and returns the accurate time.
   * 
   * @deprecated Use accurateTimeMillis()
   * 
   * @return accurate time in milliseconds
   */
  @Deprecated
  public static long accurateTime() {
    return accurateTimeMillis();
  }

  /**
   * Updates the clock and returns the accurate time in milliseconds.
   * 
   * @since 2.0.0 (existed since 1.0.0 as accurateTime)
   * @return accurate time in milliseconds
   */
  public static long accurateTimeMillis() {
    return now = System.currentTimeMillis();
  }
  
  /**
   * <p>Runnable which will regularly update the stored clock time.  This runnable is designed to 
   * run in its own dedicated thread.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  private static class ClockUpdater implements Runnable {
    private volatile boolean runnableFinished = false;
    
    @Override
    public void run() {
      try {
        synchronized (UPDATE_LOCK) {
          while (clockUpdater == this) {
            try {
              accurateTimeMillis();
              
              UPDATE_LOCK.wait(AUTOMATIC_UPDATE_FREQUENCY_IN_MS);
            } catch (InterruptedException e) {
              clockUpdater = null;  // let thread exit
              Thread.currentThread().interrupt();
            }
          }
        }
      } finally {
        runnableFinished = true;
      }
    }
  }
}

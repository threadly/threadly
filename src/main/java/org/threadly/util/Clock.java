package org.threadly.util;

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
  protected static final int AUTOMATIC_UPDATE_FREQUENCY_IN_MS = 100;
  
  private static final Object UPDATE_LOCK = new Object();
  private static volatile long now = System.currentTimeMillis();
  private static volatile boolean updateClock = false;
  
  static {
    if (UPDATE_CLOCK_AUTOMATICALLY) {
      startClockUpdateThread();
    }
  }
  
  /** 
   * Starts the clock updating automatically (used for testing).
   */
  public static void startClockUpdateThread() {
    synchronized (UPDATE_LOCK) {
      if (updateClock) {
        return;
      } else {
        updateClock = true;
        
        Thread thread = new Thread(new ClockUpdater());
        
        thread.setName("Threadly clock updater");
        thread.setDaemon(true);
        thread.start();
      }
    }
  }

  /**
   * Stops the clock from updating automatically (used for testing).
   */
  public static void stopClockUpdateThread() {
    synchronized (UPDATE_LOCK) {
      updateClock = false;
      
      UPDATE_LOCK.notifyAll();
    }
  }

  /**
   * Getter for the last known time in milliseconds.
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
  
  private Clock() {
    // don't construct
  }
  
  /**
   * Runnable which will regularly update the stored clock time.  
   * This runnable is designed to run in it's own dedicated thread.
   * 
   * @author jent - Mike Jensen
   */
  private static class ClockUpdater implements Runnable {
    @Override
    public void run() {
      synchronized (UPDATE_LOCK) {
        while (updateClock) {
          try {
            accurateTime();
            
            UPDATE_LOCK.wait(AUTOMATIC_UPDATE_FREQUENCY_IN_MS);
          } catch (InterruptedException ignored) { }
        }
      }
    }
  }
}

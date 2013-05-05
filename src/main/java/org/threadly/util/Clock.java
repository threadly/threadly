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
 */
public class Clock {
  private static final boolean UPDATE_CLOCK_AUTOMATICALLY = true;
  
  private static volatile long now = System.currentTimeMillis();
  private static volatile boolean updateClock = false;
  
  static {
    if (UPDATE_CLOCK_AUTOMATICALLY) {
      runClockUpdateThread();
    }
  }
  
  private static void runClockUpdateThread() {
    if (updateClock) {
      return;
    } else {
      updateClock = true;
      
      Thread thread = new Thread() {
          public void run() {
            while (updateClock) {
              try {
                accurateTime();
                Thread.sleep(100);
              } catch (InterruptedException ignored) { }
            }
          }
        };
  
      thread.setDaemon(true);
      thread.start();
    }
  }

  public static long lastKnownTimeMillis() {
    return now;
  }

  public static long accurateTime() {
    return now = System.currentTimeMillis();
  }
}

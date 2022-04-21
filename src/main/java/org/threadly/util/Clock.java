package org.threadly.util;

import java.util.concurrent.locks.LockSupport;

/**
 * This is a utility class for low-resolution timing which avoids frequent 
 * {@link System#currentTimeMillis()} calls (which perform poorly because they require system 
 * calls).
 * <p>
 * Each call to {@link Clock#lastKnownTimeMillis()} will return the value of 
 * {@link System#currentTimeMillis()} as of the last call to {@link Clock#accurateTimeMillis()}.  
 * This means {@link #lastKnownTimeMillis()} will only be as accurate as the frequency with which 
 * {@link #accurateTimeMillis()} is called.
 * <p>
 * In order to ensure a minimum level of accuracy, by default a thread is started to call 
 * {@link #accurateTimeMillis()} every 100 milliseconds.  This can be disabled by calling 
 * {@link #stopClockUpdateThread()}.
 * 
 * @since 1.0.0
 */
public class Clock {
  /**
   * Simple conversion of how many nanoseconds exist within a single millisecond.
   * 
   * @since 3.1.0
   */
  public static final int NANOS_IN_MILLISECOND = 1_000_000;
  /**
   * This is the frequency at which the thread which regularly updates the clock wakes up and 
   * updates the time.  Invocations to {@link #accurateForwardProgressingMillis()} and 
   * {@link #accurateTimeMillis()} both update their respective times, but this will make sure 
   * that even if those calls are not made requests to {@link #lastKnownForwardProgressingMillis()} 
   * and {@link #lastKnownTimeMillis()} have at least some level of accuracy.
   *  
   * @since 4.0.0
   */
  public static final short AUTOMATIC_UPDATE_FREQUENCY_IN_MS = 100;
  protected static final short STOP_PARK_TIME_NANOS = 25000;
  
  protected static final Object UPDATE_LOCK = new Object();
  protected static ClockUpdater clockUpdater = null;
  protected static final long CLOCK_STARTUP_TIME_NANOS = System.nanoTime();
  private static volatile long nowNanos = CLOCK_STARTUP_TIME_NANOS;
  private static volatile long nowMillis = System.currentTimeMillis();
  
  static {
    startClockUpdateThread();
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
        
        thread.setName("Threadly clock updater"); // name referenced in Profiler
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
      while (! oldUpdater.runnableFinished && ! currentThread.isInterrupted()) {
        LockSupport.parkNanos(STOP_PARK_TIME_NANOS);
      }
    }
  }
  
  /**
   * This directly returns the result of {@link System#nanoTime()}.  Using this as an alternative 
   * to invoking {@link System#nanoTime()} directly is that it updates the nano time 
   * representation, allowing for more accurate time references when calling 
   * {@link #lastKnownTimeNanos()} and {@link Clock#lastKnownForwardProgressingMillis()}.
   * <p>
   * Please read the java documentation about {@link System#nanoTime()} to understand the nature 
   * of this value (it may be positive, negative, overflow, and is completely arbitrary from its 
   * start point).
   * 
   * @return a long which is a constantly forward moving representation of nano seconds
   */
  public static long accurateTimeNanos() {
    return nowNanos = System.nanoTime();
  }
  
  /**
   * This returns a fuzzy known nano time from invocations to either {@link #accurateTimeNanos()} 
   * or {@link #accurateForwardProgressingMillis()}.  In addition (unless manually stopped via 
   * {@link #stopClockUpdateThread()}) this time is updated at the frequency of 
   * {@link #AUTOMATIC_UPDATE_FREQUENCY_IN_MS}.  Thus providing a minimal level of accuracy.
   * <p>
   * Please read the java documentation about {@link System#nanoTime()} to understand the nature 
   * of this value (it may be positive, negative, overflow, and is completely arbitrary from its 
   * start point).
   * 
   * @return a long which is a constantly forward moving representation of nano seconds
   */
  public static long lastKnownTimeNanos() {
    return nowNanos;
  }
  
  /**
   * Returns a fuzzy time for how much time in milliseconds since this class has loaded (starting 
   * at {@code 0}).  If {@link Clock} was loaded at the start of the application, this can provide 
   * the amount of time the application has been running.  
   * <p>
   * This call is guaranteed to only progress forward, regardless of system clock changes it will 
   * move forward at a consistent rate.  
   * <p>
   * By default (unless manually stopped via {@link #stopClockUpdateThread()}) this time is 
   * updated automatically at the frequency of {@link #AUTOMATIC_UPDATE_FREQUENCY_IN_MS}.  Thus 
   * allowing a guarantee of minimal accuracy within the set milliseconds.
   * 
   * @since 3.1.0
   * @return Amount of time in milliseconds since Clock class was loaded
   */
  public static long lastKnownForwardProgressingMillis() {
    /* We can not guarantee that nowNanos is > CLOCK_STARTUP_TIME_NANOS, since the nano time may 
     * overflow.  But subtracting after an overflow, will still produce a positive result.
     */
    return (nowNanos - CLOCK_STARTUP_TIME_NANOS) / NANOS_IN_MILLISECOND;
  }
  
  /**
   * Returns an accurate amount of time in milliseconds since this class has loaded (starting at 
   * {@code 0}).  If {@link Clock} was loaded at the start of the application, this can provide 
   * the amount of time the application has been running.  Calls to this will NOT update the time 
   * in {@link #accurateTimeMillis()}.  
   * <p>
   * This call is guaranteed to only progress forward, regardless of system clock changes it will 
   * move forward at a consistent rate.
   * 
   * @since 3.1.0
   * @return Amount of time in milliseconds since Clock class was loaded
   */
  public static long accurateForwardProgressingMillis() {
    return ((nowNanos = System.nanoTime()) - CLOCK_STARTUP_TIME_NANOS) / NANOS_IN_MILLISECOND;
  }
  
  /**
   * Finds the duration in milliseconds from the reference time.  Effectively this is the same as 
   * subtracting the result from {@link #lastKnownForwardProgressingMillis()} from the parameter 
   * provided.  The parameter provided should have been a result from 
   * {@link #lastKnownForwardProgressingMillis()} or {@link #accurateForwardProgressingMillis()}.  
   * Manipulating that time before calling this is not supported (negative results will not be 
   * provided).  This is simply a helper function for this very common operation.
   * 
   * @param forwardMillis The reference time
   * @return The milliseconds from the provided reference time
   */
  public static long forwardProgressingDuration(long forwardMillis) {
    return Math.max(0, lastKnownForwardProgressingMillis() - forwardMillis);
  }

  /**
   * Getter for the last known time in milliseconds.  This time is considered semi-accurate, based 
   * off the last time accurate time has been requested, or this class has automatically updated 
   * the time (unless requested to stop automatically updating).  
   * <p>
   * If the system clock goes backwards this too can go backwards.  If that is not desirable 
   * consider using {@link #lastKnownForwardProgressingMillis()}.  
   * <p>
   * By default (unless manually stopped via {@link #stopClockUpdateThread()}) this time is 
   * updated automatically at the frequency of {@link #AUTOMATIC_UPDATE_FREQUENCY_IN_MS}.  Thus 
   * allowing a guarantee of minimal accuracy within the set milliseconds.
   * 
   * @return last known time in milliseconds
   */
  public static long lastKnownTimeMillis() {
    return nowMillis;
  }

  /**
   * Updates the clock so that future calls to {@link #lastKnownTimeMillis()} can benefit, and 
   * returns the accurate time in milliseconds.  This will NOT update the time for calls to 
   * {@link #lastKnownForwardProgressingMillis()}.
   * <p>
   * If the system clock goes backwards this too can go backwards.  If that is not desirable 
   * consider using {@link #accurateForwardProgressingMillis()}.
   * 
   * @since 2.0.0 (since 1.0.0 as accurateTime)
   * @return accurate time in milliseconds
   */
  public static long accurateTimeMillis() {
    return nowMillis = System.currentTimeMillis();
  }
  
  /**
   * Runnable which will regularly update the stored clock time.  This runnable is designed to run 
   * in its own dedicated thread.
   * 
   * @since 1.0.0
   */
  protected static class ClockUpdater implements Runnable {
    protected volatile boolean runnableFinished = false;
    protected long lastUpdatedMillis = -1;
    protected long lastUpdatedNanos = -1;
    
    @Override
    public void run() {
      try {
        synchronized (UPDATE_LOCK) {
          while (clockUpdater == this) {
            try {
              if (nowMillis == lastUpdatedMillis || 
                  // check if task is not waking up as expected, if not lets update while we have cpu time
                  nowMillis - lastUpdatedMillis > AUTOMATIC_UPDATE_FREQUENCY_IN_MS) {
                nowMillis = lastUpdatedMillis = System.currentTimeMillis();
              } else {
                lastUpdatedMillis = nowMillis;
              }
              if (nowNanos == lastUpdatedNanos || 
                  // check if task is not waking up as expected, if not lets update while we have cpu time
                  nowNanos - lastUpdatedNanos > AUTOMATIC_UPDATE_FREQUENCY_IN_MS * NANOS_IN_MILLISECOND) {
                nowNanos = lastUpdatedNanos = System.nanoTime();
              } else {
                lastUpdatedNanos = nowNanos;
              }
              
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

package org.threadly.concurrent;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * Class for helping calculate the offset for scheduling tasks.  For example if you want a task to 
 * run at 10 minutes after the hour, every hour, you can use {@link #getDelayTillMinute(int)} to 
 * calculate the initial delay needed when scheduling with 
 * {@link SubmitterScheduler#scheduleAtFixedRate(Runnable, long, long)}, and then provide 1 hour 
 * in milliseconds for the "period".
 * 
 * @since 3.5.0
 */
public class SchedulingUtils {
  protected static volatile int cachedHourShift = Integer.MIN_VALUE;
  
  /**
   * Call to calculate how many milliseconds until the provided minute.  If we are past the 
   * provided minute, it will be the milliseconds until we reach that minute with the NEXT hour.  
   * <p>
   * Because of use of {@link Clock#lastKnownTimeMillis()}, this calculation will only be accurate 
   * within about 100 milliseconds.  Of course if provided to a scheduler, depending on it's work 
   * load that variation may be greater.
   * 
   * @param minute Minute to calculate too, can not be negative and must be less than 60
   * @return Time in milliseconds till that minute is reached
   */
  public static long getDelayTillMinute(int minute) {
    ArgumentVerifier.assertLessThan(minute, TimeUnit.HOURS.toMinutes(1), "minute");
    ArgumentVerifier.assertNotNegative(minute, "minute");

    return getDelayTillMinute(Clock.lastKnownTimeMillis(), minute);
  }

  /**
   * Call to calculate how many milliseconds until the provided minute,  If we are past the 
   * provided minute, it will be the milliseconds until we reach that minute with the NEXT hour.  
   * <p>
   * Because of use of {@link Clock#lastKnownTimeMillis()}, this calculation will only be accurate 
   * within about 100 milliseconds.  Of course if provided to a scheduler, depending on it's work 
   * load that variation may be greater.
   * 
   * @param now Current time in milliseconds since epoc
   * @param minute Minute to calculate too, can not be negative and must be less than 60
   * @return Time in milliseconds till that minute is reached
   */
  protected static long getDelayTillMinute(long now, int minute) {
    long currentMin = TimeUnit.MILLISECONDS.toMinutes(now % TimeUnit.HOURS.toMillis(1));
    
    long minutesToWait = 0;
    if (minute > currentMin) {
      minutesToWait = minute - currentMin;
    } else if (minute <= currentMin) {
      minutesToWait = TimeUnit.HOURS.toMinutes(1) - currentMin + minute;
    }
    
    // subtract seconds that have passed in current minute
    long offset = now % TimeUnit.MINUTES.toMillis(1);
    return TimeUnit.MINUTES.toMillis(minutesToWait) - offset;
  }
  
  /**
   * Call to calculate how many milliseconds until the provided time.  If we are past the 
   * provided hour/minute, it will be the milliseconds until we reach that time with the NEXT day.  
   * <p>
   * It is important to note that the time zone for this hour is UTC.  If you want to use this for 
   * local time, just pass the hour through {@link #shiftLocalHourToUTC(int)}.  This will convert 
   * a local time's hour to UTC so that it can be used in this invocation.  
   * <p>
   * Because of use of {@link Clock#lastKnownTimeMillis()}, this calculation will only be accurate 
   * within about 100 milliseconds.  Of course if provided to a scheduler, depending on it's work 
   * load that variation may be greater.
   * 
   * @param hour Hour in the 24 hour format, can not be negative and must be less than 24
   * @param minute Minute to calculate too, can not be negative and must be less than 60
   * @return Time in milliseconds till provided time is reached
   */
  public static long getDelayTillHour(int hour, int minute) {
    ArgumentVerifier.assertLessThan(hour, TimeUnit.DAYS.toHours(1), "hour");
    ArgumentVerifier.assertNotNegative(hour, "hour");
    ArgumentVerifier.assertLessThan(minute, TimeUnit.HOURS.toMinutes(1), "minute");
    ArgumentVerifier.assertNotNegative(minute, "minute");
    
    return getDelayTillHour(Clock.lastKnownTimeMillis(), hour, minute);
  }
  
  /**
   * Call to calculate how many milliseconds until the provided time.  If we are past the 
   * provided hour/minute, it will be the milliseconds until we reach that time with the NEXT day.  
   * <p>
   * It is important to note that the time zone for this hour is UTC.  If you want to use this for 
   * local time, just pass the hour through {@link #shiftLocalHourToUTC(int)}.  This will convert 
   * a local time's hour to UTC so that it can be used in this invocation.  
   * <p>
   * Because of use of {@link Clock#lastKnownTimeMillis()}, this calculation will only be accurate 
   * within about 100 milliseconds.  Of course if provided to a scheduler, depending on it's work 
   * load that variation may be greater.
   * 
   * @param now Current time in milliseconds since epoc
   * @param hour Hour in the 24 hour format, can not be negative and must be less than 24
   * @param minute Minute to calculate too, can not be negative and must be less than 60
   * @return Time in milliseconds till provided time is reached
   */
  protected static long getDelayTillHour(long now, int hour, int minute) {
    long delayInMillis = TimeUnit.MINUTES.toMillis(minute);
    long currentHour = TimeUnit.MILLISECONDS.toHours(now % TimeUnit.DAYS.toMillis(1));
    
    if (hour > currentHour) {
      delayInMillis += TimeUnit.HOURS.toMillis(hour - currentHour);
    } else if (hour < currentHour) {
      delayInMillis += TimeUnit.HOURS.toMillis(TimeUnit.DAYS.toHours(1) - currentHour + hour);
    } else {
      long result = getDelayTillMinute(Clock.lastKnownTimeMillis(), minute);
      if (TimeUnit.MILLISECONDS.toMinutes(result) <= minute) {
        return result;
      } else {
        // here we have to add the time to forward us to the next day
        return result + TimeUnit.HOURS.toMillis(TimeUnit.DAYS.toHours(1) - 1);
      }
    }

    // subtract minutes, seconds, and milliseconds that have passed
    long offset = now % TimeUnit.HOURS.toMillis(1);
    return delayInMillis - offset;
  }
  
  /**
   * This will shift an hour from the local time zone to UTC.  This shift will take into account 
   * the current local state of daylight savings time.  The primary usage of this is so that 
   * {@link #getDelayTillHour(int, int)} can be used with a local time zone hour.
   * 
   * @param hour Hour to be shifted in the local time zone in 24 hour format
   * @return Hour shifted to the UTC time zone in 24 hour format
   */
  public static int shiftLocalHourToUTC(int hour) {
    ArgumentVerifier.assertLessThan(hour, TimeUnit.DAYS.toHours(1), "hour");
    ArgumentVerifier.assertNotNegative(hour, "hour");
    
    if (cachedHourShift == Integer.MIN_VALUE) {
      Calendar calendar = Calendar.getInstance();
      int shiftInMillis = calendar.get(Calendar.ZONE_OFFSET) + calendar.get(Calendar.DST_OFFSET);
      cachedHourShift = (int)(shiftInMillis / TimeUnit.HOURS.toMillis(1));
    }

    hour -= cachedHourShift;
    if (hour > TimeUnit.DAYS.toHours(1) - 1) {
      hour %= TimeUnit.DAYS.toHours(1);
    } else if (hour < 0) {
      hour += TimeUnit.DAYS.toHours(1);
    }
    
    return hour;
  }
}

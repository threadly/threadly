package org.threadly.util.debug;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.threadly.util.Clock;

/**
 * <p>Often times when trying to understand a concurrency issue, adding logging may solve that 
 * problem.  This class is designed to help work around that problem in some situations.  It works 
 * by not actually outputting the logs collected, but storing them in a concurrent structure.</p>
 * 
 * <p>It will ensure that when your ready to dump all the logs, they will be returned in the order 
 * they were provided.  Since these are not outputted to the actual log stream, make sure any 
 * logging relevant to the issue is captured by this utility.</p>
 * 
 * <p>This utility has several deficiencies, the largest of which is using System.nanoTime() for 
 * log ordering.  Since nanosecond time can roll over from positive to negative, in those rare 
 * situations log ordering may be incorrect.  It is design only as a debugging aid and should 
 * NEVER be included after debugging is completed.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class DebugLogger {
  protected static final boolean LOG_TIME_DEFAULT = false;
  protected static final String TIME_DELIM = " - ";
  protected static volatile ConcurrentSkipListMap<Long, String> logMap = new ConcurrentSkipListMap<>();
  
  /**
   * This adds a log message to the stored log.  Keep in mind this will continue to consume more 
   * and more memory until {@link #getAllStoredMessages()} is called.
   * 
   * @param msg message to be stored into log map
   */
  public static void log(String msg) {
    long time = Clock.accurateTimeNanos();
    String replacement = logMap.putIfAbsent(time, msg);
    while (replacement != null) {
      replacement = logMap.putIfAbsent(++time, msg);
    }
  }
  
  /**
   * This call checks how many messages are waiting in the stored map.  This can be useful if you 
   * are possibly storing lots of messages and may need to know when to regularly drain the map.
   * 
   * @return current number of stored log messages
   */
  public static int getCurrentMessageQty() {
    return logMap.size();
  }
  
  /**
   * Request to get and clear all currently stored log messages.  This will return all the log 
   * messages formatted into a single string, separated by new line characters.  
   * 
   * This calls {@link #getAllStoredMessages(boolean)} with a default of NOT including the time in 
   * nanoseconds.
   * 
   * @return string with all log messages, separated by a new line
   */
  public static String getAllStoredMessages() {
    return getAllStoredMessages(LOG_TIME_DEFAULT);
  }
  
  /**
   * Request to get and clear all currently stored log messages.  This will return all the log 
   * messages formatted into a single string, separated by new line characters.
   * 
   * @param includeLogTimes boolean to include time in nanoseconds that log message was recorded
   * @return string with all log messages, separated by a new line
   */
  public static String getAllStoredMessages(boolean includeLogTimes) {
    ConcurrentSkipListMap<Long, String> currentLog = logMap;
    logMap = new ConcurrentSkipListMap<>();
    
    try {
      Thread.sleep(100);  // wait for any possibly log messages attempting to be currently stored
    } catch (InterruptedException e) {
      // go ahead and produce results now and quit quickly
      Thread.currentThread().interrupt();
    }
    
    StringBuilder result = new StringBuilder();
    Iterator<Entry<Long, String>> it = currentLog.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Long, String> entry = it.next();
      if (includeLogTimes) {
        result.append(entry.getKey())
              .append(TIME_DELIM)
              .append(entry.getValue());
      } else {
        result.append(entry.getValue());
      }
      if (it.hasNext()) {
        result.append(System.lineSeparator());
      }
    }
    
    return result.toString();
  }
  
  
  /**
   * This call retrieves and removes the oldest stored log messages.  It will only return at most 
   * the maximum quantity provided, but may return less if not that many messages are currently 
   * available.  This call is slightly less efficient than {@link #getAllStoredMessages()}.
   * 
   * This calls {@link #getOldestLogMessages(int, boolean)} with a default of NOT including the 
   * time in nanoseconds.
   * 
   * @param qty maximum quantity of messages to retrieve
   * @return string with requested log messages, separated by a new line
   */
  public static String getOldestLogMessages(int qty) {
    return getOldestLogMessages(qty, LOG_TIME_DEFAULT);
  }
  
  /**
   * This call retrieves and removes the oldest stored log messages.  It will only return at most 
   * the maximum quantity provided, but may return less if not that many messages are currently 
   * available.  This call is slightly less efficient than {@link #getAllStoredMessages()}.
   * 
   * @param qty maximum quantity of messages to retrieve
   * @param includeLogTimes boolean to include time in nanoseconds that log message was recorded
   * @return string with requested log messages, separated by a new line
   */
  public static String getOldestLogMessages(int qty, boolean includeLogTimes) {
    int collectedQty = 0;
    StringBuilder result = new StringBuilder();
    Iterator<Entry<Long, String>> it = logMap.entrySet().iterator();
    while (it.hasNext() && collectedQty < qty) {
      Entry<Long, String> entry = it.next();
      if (includeLogTimes) {
        result.append(entry.getKey())
              .append(TIME_DELIM)
              .append(entry.getValue());
      } else {
        result.append(entry.getValue());
      }
      if (it.hasNext()) {
        result.append(System.lineSeparator());
      }
      
      collectedQty++;
      it.remove();
    }
    
    return result.toString();
  }
}

package org.threadly.util;

import java.util.Random;

/**
 * <p>Some small utilities and constants around handling strings.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.1.0
 */
public class StringUtils {
  /**
   * Constant to represent the line separator for the system (captured from 
   * System.getProperty("line.separator")).
   */
  public static final String NEW_LINE;
  
  static {
    String newLine;
    try {
      newLine = System.getProperty("line.separator");
    } catch (SecurityException e) {
      ExceptionUtils.handleException(e);
      newLine = "\n";
    }
    NEW_LINE = newLine;
  }
  
  protected static final String RAND_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  protected static final Random RANDOM = new Random(Clock.lastKnownTimeMillis());
  
  /**
   * Makes sure a given string is not {@code null}.  If it is not {@code null}, the provided string 
   * is immediately returned.  If it IS {@code null}, then an empty string is returned.
   * 
   * @since 4.2.0
   * @param input String which should be returned if not {@code null}
   * @return The original string if not {@code null}, otherwise an empty string
   */
  public static String nullToEmpty(String input) {
    if (input != null) {
      return input;
    } else {
      return "";
    }
  }

  /**
   * Converts an empty string into a {@code null}.  If it is not empty or {@code null}, the 
   * provided string is immediately returned.
   * 
   * @since 4.2.0
   * @param input String which should be returned if not {@code null} or empty
   * @return The original string if not empty, otherwise {@code null}
   */
  public static String emptyToNull(String input) {
    if (input == null || input.isEmpty()) {
      return null;
    } else {
      return input;
    }
  }
  
  /**
   * Check if the provided string is either {@code null} or empty.
   * 
   * @since 4.2.0
   * @param input String to check against
   * @return {@code true} if the provided string is {@code null} or has no content
   */
  public static boolean isNullOrEmpty(String input) {
    return input == null || input.isEmpty();
  }
  
  /**
   * Pads the start of the provided string with the provided character until a minimum length is 
   * reached.  If the provided string is greater than or equal to the minLength it will be returned 
   * without modification.  If the provided string is {@code null} it will be returned as an empty 
   * string, padded to the minimum length.
   * 
   * @since 4.2.0
   * @param sourceStr String to start from, this will be the end of the returned result string
   * @param minLength Minimum number of characters the returned string should be
   * @param padChar Character to pad on to the start of to reach the minimum length
   * @return A non-null string that is at least the length requested
   */
  public static String padStart(String sourceStr, int minLength, char padChar) {
    sourceStr = nullToEmpty(sourceStr);
    if (sourceStr.length() >= minLength) {
      return sourceStr;
    } else {
      StringBuilder sb = new StringBuilder(minLength);
      int padCount = minLength - sourceStr.length();
      while (sb.length() < padCount) {
        sb.append(padChar);
      }
      sb.append(sourceStr);
      return sb.toString();
    }
  }

  
  /**
   * Pads the end of the provided string with the provided character until a minimum length is 
   * reached.  If the provided string is greater than or equal to the minLength it will be returned 
   * without modification.  If the provided string is {@code null} it will be returned as an empty 
   * string, padded to the minimum length.
   * 
   * @since 4.2.0
   * @param sourceStr String to start from, this will be the start of the returned result string
   * @param minLength Minimum number of characters the returned string should be
   * @param padChar Character to pad on to the end of to reach the minimum length
   * @return A non-null string that is at least the length requested
   */
  public static String padEnd(String sourceStr, int minLength, char padChar) {
    sourceStr = nullToEmpty(sourceStr);
    if (sourceStr.length() >= minLength) {
      return sourceStr;
    } else {
      StringBuilder sb = new StringBuilder(minLength);
      sb.append(sourceStr);
      while (sb.length() < minLength) {
        sb.append(padChar);
      }
      return sb.toString();
    }
  }
  
  /**
   * Produces a random string of the provided length.  This can be useful for unit testing, or any 
   * other time the string content is not important.  The returned string will be comprised of 
   * only alphanumeric characters.
   * 
   * @param length Number of characters the resulting string should be.
   * @return A string comprised of random characters of the specified length
   */
  public static String makeRandomString(int length) {
     StringBuilder sb = new StringBuilder(length);
     
     for(int i = 0; i < length; i++) {
       int randIndex = RANDOM.nextInt(RAND_CHARS.length());
       char randChar = RAND_CHARS.charAt(randIndex);
       sb.append(randChar);
     }
     
     return sb.toString();
  }
}

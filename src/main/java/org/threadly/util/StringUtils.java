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
   * Constant for a single reference to an empty string.
   */
  public static final String EMPTY = "";
  /**
   * Constant to represent the line separator for the system (captured from System.getProperty("line.separator")).
   */
  public static final String NEW_LINE = System.getProperty("line.separator");
  
  private static final String RAND_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final Random RANDOM = new Random(Clock.lastKnownTimeMillis());
  
  /**
   * Makes sure a given string is not null.  If it is not null, the provided string is immediately 
   * returned.  If it IS null, then an empty string is returned.
   * 
   * @param input String which should be returned if not null
   * @return The original string if not null, otherwise an empty string
   */
  public static String makeNonNull(String input) {
    if (input != null) {
      return input;
    } else {
      return EMPTY;
    }
  }
  
  /**
   * Produces a random string of the provided length.  This can be useful for unit testing, or any 
   * other time the string content is not important.  The returned string will be comprised of only 
   * alphanumeric characters.
   * 
   * @param length Number of characters the resulting string should be.
   * @return A string comprised of random characters of the specified length
   */
  public static String randomString(int length) {
     StringBuilder sb = new StringBuilder(length);
     
     for(int i = 0; i < length; i++) {
       int randIndex = RANDOM.nextInt(RAND_CHARS.length());
       char randChar = RAND_CHARS.charAt(randIndex);
       sb.append(randChar);
     }
     
     return sb.toString();
  }
}

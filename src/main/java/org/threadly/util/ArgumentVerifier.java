package org.threadly.util;

/**
 * <p>Simple class to do some argument verifying which is common within threadly.  This 
 * is designed to primarily reduce bulk/repeated code throughout the base, as well as to 
 * ensure that thrown exceptions have a common format for the exception message.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.4.0
 */
public class ArgumentVerifier {
  /**
   * Verifies the provided argument is not null, if it is an IllegalArgumentException is thrown.
   * 
   * @param arg Object to check against to not be null
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if the arg is null
   */
  public static void assertNotNull(Object arg, String name) {
    if (arg == null) {
      throw new IllegalArgumentException(StringUtils.makeNonNull(name) + 
                                           " can not be null");
    }
  }
  
  /**
   * Verifies the provided argument is not negative (zero is okay).  If it is less than zero 
   * an IllegalArgumentException is thrown.
   * 
   * @param val Value to check against
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if value is less than zero
   */
  public static void assertNotNegative(long val, String name) {
    if (val < 0) {
      throw new IllegalArgumentException(StringUtils.makeNonNull(name) + 
                                           " can not be negative");
    }
  }
  
  /**
   * Verifies the provided argument is greater than zero.  If it is less than one 
   * an IllegalArgumentException is thrown.
   * 
   * @param val Value to check against
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if value is less than one
   */
  public static void assertGreaterThanZero(long val, String name) {
    if (val < 1) {
      throw new IllegalArgumentException(StringUtils.makeNonNull(name) + 
                                           " must be > 0");
    }
  }
}

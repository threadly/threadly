package org.threadly.util;

/**
 * Simple class to do some argument verifying which is common within threadly.  This is designed 
 * to primarily reduce bulk/repeated code throughout the base, as well as to ensure that thrown 
 * exceptions have a common format for the exception message.
 * 
 * @since 2.4.0
 */
public class ArgumentVerifier {
  /**
   * Verifies the provided argument is not null, if it is an {@link IllegalArgumentException} is 
   * thrown.
   * 
   * @param arg Object to check against to not be {@code null}
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if the arg is {@code null}
   */
  public static void assertNotNull(Object arg, String name) {
    if (arg == null) {
      throw new IllegalArgumentException(StringUtils.nullToEmpty(name) + " can not be null");
    }
  }
  
  /**
   * Verifies the provided argument is not negative (zero is okay).  If it is less than zero an 
   * {@link IllegalArgumentException} is thrown.
   * 
   * @param val Value to verify against
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if value is less than zero
   */
  public static void assertNotNegative(double val, String name) {
    if (val < 0) {
      throw new IllegalArgumentException(StringUtils.nullToEmpty(name) + " can not be negative: " + val);
    }
  }
  
  /**
   * Verifies the provided argument is greater than zero.  If it is less than one an 
   * {@link IllegalArgumentException} is thrown.
   * 
   * @param val Value to verify against
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if value is less than one
   */
  public static void assertGreaterThanZero(double val, String name) {
    if (val <= 0) {
      throw new IllegalArgumentException(StringUtils.nullToEmpty(name) + " must be > 0: " + val);
    }
  }
  
  /**
   * Verifies the provided argument is less than the compare value.  If it is greater than or 
   * equal to the compare value an {@link IllegalArgumentException} is thrown.
   * 
   * @param val Value to verify
   * @param compareVal value to compare against
   * @param name Name to provide the argument in the message of the thrown exception
   * @throws IllegalArgumentException Thrown if value is greather than or equal to compareVal
   */
  public static void assertLessThan(double val, double compareVal, String name) {
    if (val >= compareVal) {
      throw new IllegalArgumentException(StringUtils.nullToEmpty(name) + 
                                           " must be < " + compareVal + ": " + val);
    }
  }
}

package org.threadly.util;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * Some small utilities and constants around handling strings.
 * 
 * @since 2.1.0
 */
public class StringUtils {
  protected static final String RAND_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  
  /**
   * Check to see if all characters in a provided string match to a given {@link Predicate}.  This 
   * can be easily used when provided predicates from {@link Character}.  For example providing 
   * {@code Character::isLetter} for the predicate will verify all characters in the string are 
   * letters.
   * 
   * @since 5.14
   * @param p Character test
   * @param s CharSequence to check against
   * @return {@code true} if predicate returned true for every character in string
   */
  public static boolean allCharsMatch(Predicate<Character> p, CharSequence s) {
    for (int i = 0; i < s.length(); i++) {
      if (! p.test(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Check to see if any characters in a provided string match to a given {@link Predicate}.  This 
   * can be easily used when provided predicates from {@link Character}.  For example providing 
   * {@code Character::isDigit} for the predicate to see if this string contains any numbers.
   * 
   * @since 5.14
   * @param p Character test
   * @param s CharSequence to check against
   * @return {@code true} if predicate returned true for any characters in the string
   */
  public static boolean anyCharsMatch(Predicate<Character> p, CharSequence s) {
    for (int i = 0; i < s.length(); i++) {
      if (p.test(s.charAt(i))) {
        return true;
      }
    }
    return false;
  }
  
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
   * Simple utility function for returning an {@link Optional} that if contents present are 
   * guaranteed to not be empty.  Basically taking the empty case into consideration in addition 
   * to {@link Optional#ofNullable(Object)}'s normal {@code null} check.
   * 
   * @since 5.4
   * @param input String to be contained in returned {@link Optional} if not null or empty
   * @return Optional which if present contains a non-empty String
   */
  public static Optional<String> nonEmptyOptional(String input) {
    if (isNullOrEmpty(input)) {
      return Optional.empty();
    } else {
      return Optional.of(input);
    }
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
       char randChar = RAND_CHARS.charAt(ThreadLocalRandom.current().nextInt(RAND_CHARS.length()));
       sb.append(randChar);
     }
     
     return sb.toString();
  }
}

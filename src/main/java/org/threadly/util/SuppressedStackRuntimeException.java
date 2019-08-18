package org.threadly.util;

/**
 * Type of {@link RuntimeException} which does not generate a stack at it's point of creation.  
 * Generating a stack trace in java is very expensive, and does not always further the understanding 
 * of the type of error (particularly when the exception is wrapping another exception, or is a 
 * communication of state).  In those select conditions using or extending this type of exception 
 * can provide a significant performance gain.
 * 
 * @deprecated Renamed to {@link StackSuppressedRuntimeException}
 * 
 * @since 4.8.0
 */
@Deprecated
public class SuppressedStackRuntimeException extends StackSuppressedRuntimeException {
  private static final long serialVersionUID = -3253477627669379892L;
  
  /**
   * Construct a new exception with no message or cause.  The cause is not initialized, and may 
   * subsequently be initialized by invoking {@link #initCause}.
   */
  public SuppressedStackRuntimeException() {
    super();
  }

  /**
   * Construct a new exception with a provided message and no cause.
   * 
   * @param msg The message which can later be retrieved by {@link #getMessage()}
   */
  public SuppressedStackRuntimeException(String msg) {
    super(msg);
  }

  /**
   * Construct a new exception with a provided cause.  The message will be defaulted from the cause 
   * provided.
   * 
   * @param cause The cause which contributed to this exception
   */
  public SuppressedStackRuntimeException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct a new exception providing both a unique message and cause.
   * 
   * @param msg The message which can later be retrieved by {@link #getMessage()}
   * @param cause The cause which contributed to this exception
   */
  public SuppressedStackRuntimeException(String msg, Throwable cause) {
    super(msg, cause);
  }
}

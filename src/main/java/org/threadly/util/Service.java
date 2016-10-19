package org.threadly.util;

/**
 * A service is defined as something which is constructed in a stopped state (unless the 
 * constructor starts the service automatically).  It is then at some point started, and at some 
 * future point stopped.  Once stopped it is expected that this "Service" can no longer be used.
 * 
 * @since 4.3.0 (since 3.8.0 as ServiceInterface)
 */
public interface Service {
  /**
   * Starts the service, blocking until the service is running.
   * 
   * @throws IllegalStateException thrown if the service has already been started
   */
  public void start() throws IllegalStateException;
  
  /**
   * Starts the service if it has not already been started.  If the service has been started this 
   * invocation will do nothing (except return {@code false}).  If this call starts the service 
   * the thread will block until the service is running.
   * 
   * @return {@code true} if the service has been started from this call
   */
  public boolean startIfNotStarted();
  
  /**
   * Stops the service, blocking until the service is shutdown.
   * 
   * @throws IllegalStateException thrown if the service has never been started, or is already shutdown
   */
  public void stop();
  
  /**
   * Stops the service if it currently running.  If the service has been stopped already, or never 
   * started this invocation will do nothing (except return {@code false}).  If this call stops 
   * the service the thread will block until the service is shutdown.
   * 
   * @return {@code true} if the service has been stopped from this call
   */
  public boolean stopIfRunning();
  
  /**
   * Call to check if the service has been started, and not shutdown yet.  If you need a check 
   * that will be consistent while both new and while running please see {@link #hasStopped()}.
   * 
   * @return {@code true} if the service is currently running
   */
  public boolean isRunning();
  
  /**
   * Call to check if the service has been stopped (and thus can no longer be used).  This is 
   * different from {@link #isRunning()} in that it will return {@code false} until 
   * {@link #stop()} or {@link #stopIfRunning()} has been invoked.  Thus allowing you to make a 
   * check that's state will be consistent when it is new and while it is running.
   * 
   * @return {@code true} if the server has been stopped
   */
  public boolean hasStopped();
}

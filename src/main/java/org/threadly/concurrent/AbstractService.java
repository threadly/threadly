package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>An abstract implementation of a "Service".  A service is defined as something which is 
 * constructed in a stopped state.  It is then at some point started, and at some future point 
 * stopped.  Once stopped it is expected that this "Service" can no longer be used.</p>
 * 
 * <p>This implementation is flexible, weather the internal service is scheduled on a thread pool 
 * runs on a unique thread, or has other means of running.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.6.0
 */
public abstract class AbstractService {
  private AtomicInteger state = new AtomicInteger(0); // 0 = new, 1 = started, 2 = stopped
  
  /**
   * Starts the service, blocking until the service is running.
   * 
   * @throws IllegalStateException thrown if the service has already been started
   */
  public void start() throws IllegalStateException {
    if (! startIfNotStarted()) {
      throw new IllegalStateException();
    }
  }
  
  /**
   * Starts the service if it has not already been started.  If the service has been started this 
   * invocation will do nothing (except return {@code false}).  If this call starts the service 
   * the thread will block until the service is running.
   * 
   * @return {@code true} if the service has been started from this call
   */
  public boolean startIfNotStarted() {
    if (state.get() == 0 && state.compareAndSet(0, 1)) {
      startupService();
      
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Called internally when the service should perform any actions to startup.  It is expected 
   * that this call will block until the service is running.  This invocation will only occur 
   * once.
   */
  protected abstract void startupService();

  /**
   * Stops the service, blocking until the service is shutdown.
   * 
   * @throws IllegalStateException thrown if the service has never been started, or is already shutdown
   */
  public void stop() {
    if (! stopIfRunning()) {
      throw new IllegalStateException();
    }
  }
  
  /**
   * Stops the service if it currently running.  If the service has been stopped already, or never 
   * started this invocation will do nothing (except return {@code false}).  If this call stops 
   * the service the thread will block until the service is shutdown.
   * 
   * @return {@code true} if the service has been stopped from this call
   */
  public boolean stopIfRunning() {
    if (state.get() == 1 && state.compareAndSet(1, 2)) {
      shutdownService();
      
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Called internally when the service should perform any actions to shutdown.  It is expected 
   * that this call will block until the service is shutdown.  This invocation will only occur 
   * once.
   */
  protected abstract void shutdownService();
  
  /**
   * Call to check if the service has been started, and not shutdown yet.
   * 
   * @return {@code true} if the service is currently running
   */
  public boolean isRunning() {
    return state.get() == 1;
  }
}

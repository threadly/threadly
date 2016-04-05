package org.threadly.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>An abstract implementation of the {@link Service} interface.</p>
 * 
 * <p>This implementation is flexible, weather the internal service is scheduled on a thread pool 
 * runs on a unique thread, or has other means of running.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.6.0
 */
public abstract class AbstractService implements Service {
  private AtomicInteger state = new AtomicInteger(0); // 0 = new, 1 = started, 2 = stopped
  
  @Override
  public void start() throws IllegalStateException {
    if (! startIfNotStarted()) {
      throw new IllegalStateException();
    }
  }
  
  @Override
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
  
  @Override
  public void stop() {
    if (! stopIfRunning()) {
      throw new IllegalStateException();
    }
  }
  
  @Override
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
  
  @Override
  public boolean isRunning() {
    return state.get() == 1;
  }
  
  @Override
  public boolean hasStopped() {
    return state.get() == 2;
  }
}

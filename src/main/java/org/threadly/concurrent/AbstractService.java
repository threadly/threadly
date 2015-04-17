package org.threadly.concurrent;

/**
 * <p>An abstract implementation of a "Service".  A service is defined as something which is 
 * constructed in a stopped state.  It is then at some point started, and at some future point 
 * stopped.  Once stopped it is expected that this "Service" can no longer be used.</p>
 * 
 * <p>This implementation is flexible, weather the internal service is scheduled on a thread pool 
 * runs on a unique thread, or has other means of running.</p>
 * 
 * @deprecated This has moved, switch to use {@link org.threadly.util.AbstractService}
 * 
 * @author jent - Mike Jensen
 * @since 2.6.0
 */
@Deprecated
public abstract class AbstractService extends org.threadly.util.AbstractService {
  // nothing added here
}

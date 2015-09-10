package org.threadly.util;

/**
 * <p>A service is defined as something which is constructed in a stopped state (unless the 
 * constructor starts the service automatically).  It is then at some point started, and at some 
 * future point stopped.  Once stopped it is expected that this "Service" can no longer be used.</p>
 * 
 * @deprecated Use {@link Service} as a direct replacement
 * 
 * @author jent - Mike Jensen
 * @since 3.8.0
 */
@Deprecated
public interface ServiceInterface extends Service {
  // nothing added here
}

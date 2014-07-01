package org.threadly.concurrent;

/**
 * <p>Class which assist with holding and calling to Runnable listeners.  In parallel 
 * designs it is common to have things subscribe for actions to occur (to later be 
 * alerted once an action occurs).  This class makes it easy to allow things to 
 * register as a listener.</p>
 * 
 * @deprecated use replacement at org.threadly.concurrent.event.ListenerHelper
 * 
 * @author jent - Mike Jensen
 * @since 1.1.0
 */
@Deprecated
public class ListenerHelper extends org.threadly.concurrent.event.ListenerHelper {
  /**
   * Constructs a new {@link ListenerHelper}.  This can call listeners 
   * one time, or every time callListeners is called.
   * 
   * @param callListenersOnce true if listeners should only be called once
   */
  public ListenerHelper(boolean callListenersOnce) {
    super(callListenersOnce);
  }
}

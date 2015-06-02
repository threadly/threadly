package org.threadly.concurrent;

/**
 * <p>Runnable implementation which does no action.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.1.0
 */
public class DoNothingRunnable implements Runnable {
  @Override
  public void run() {
    // as the name suggests, do nothing
  }
}

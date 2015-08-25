package org.threadly.concurrent;

/**
 * <p>Interface for a delayed task.  This is different from {@link java.util.concurrent.Delayed} 
 * because that interface implicitly requires a call to the system clock.  Causing issues when 
 * comparing two delayed tasks if the system clock changes (not to mention expensive without 
 * cause).</p>
 * 
 * <p>This solve that by just asking for the absolute time, which should be referenced from 
 * {@link org.threadly.util.Clock#accurateForwardProgressingMillis()}.  This should NOT 
 * reference the time since epoc (since that time is sensitive to clock changes).</p>
 * 
 * @author jent
 * @since 4.3.0 (since 4.2.0 as DelayedTaskInterface)
 */
interface DelayedTask {
  /**
   * Get the absolute time when this should run, in comparison with the time returned from 
   * {@link org.threadly.util.Clock#accurateForwardProgressingMillis()}.
   * 
   * @return Absolute time in millis this task should run
   */
  public long getRunTime();
}

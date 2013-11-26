package org.threadly.concurrent;

/**
 * <p>Priority to go with tasks when being submitted into implementations 
 * of {@link PrioritySchedulerInterface}.</p>
 * 
 * @author jent - Mike Jensen
 */
public enum TaskPriority { 
  @SuppressWarnings("javadoc")
  High, 
  @SuppressWarnings("javadoc")
  Low;
}
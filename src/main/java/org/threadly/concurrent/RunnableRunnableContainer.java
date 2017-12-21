package org.threadly.concurrent;

/**
 * Simple combination of {@link RunnableContainer} and {@link Runnable}.  This allows us to 
 * specify two possible run implementations while have a collection of {@link RunnableContainer}'s 
 * so that we can do task removal easily.
 * 
 * @since 5.8
 */
public interface RunnableRunnableContainer extends RunnableContainer, Runnable {
  // intentionally left blank
}
package org.threadly.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.ConfigurableThreadFactory.ConfigurableThreadFactoryBuilder;
import org.threadly.util.ExceptionHandler;

@SuppressWarnings("javadoc")
public class ThreadReferencingThreadFactoryTest extends ConfigurableThreadFactoryTest {
  @Override
  protected ConfigurableThreadFactoryBuilder makeBuilder() {
    return ThreadReferencingThreadFactory.builder();
  }
  
  @Override
  protected ThreadReferencingThreadFactory makeThreadFactory() {
    return new ThreadReferencingThreadFactory();
  }
  
  @Override
  protected ThreadReferencingThreadFactory makeThreadFactory(String poolPrefix, boolean appendPoolId) {
    return new ThreadReferencingThreadFactory(poolPrefix, appendPoolId);
  }
  
  @Override
  protected ThreadReferencingThreadFactory makeThreadFactory(boolean daemon) {
    return new ThreadReferencingThreadFactory(daemon);
  }
  
  @Override
  protected ThreadReferencingThreadFactory makeThreadFactory(int threadPriority) {
    return new ThreadReferencingThreadFactory(threadPriority);
  }
  
  @Override
  protected ThreadReferencingThreadFactory makeThreadFactory(UncaughtExceptionHandler ueh) {
    return new ThreadReferencingThreadFactory(ueh);
  }

  @Override
  protected ThreadReferencingThreadFactory makeThreadFactory(ExceptionHandler eh) {
    return new ThreadReferencingThreadFactory(eh);
  }

  @Override
  protected ThreadReferencingThreadFactory makeNotifyingThreadFactory(Consumer<Thread> threadConsumer) {
    return new ThreadReferencingThreadFactory(threadConsumer);
  }
  
  @Test
  public void getThreadsTest() {
    ThreadReferencingThreadFactory tf = makeThreadFactory();
    Thread thread1 = tf.newThread(DoNothingRunnable.instance());
    Thread thread2 = tf.newThread(DoNothingRunnable.instance());
    
    List<Thread> result = tf.getThreads(false);
    
    assertEquals(2, result.size());
    assertTrue(result.contains(thread1));
    assertTrue(result.contains(thread2));
  }
  
  @Test
  public void getAliveThreadsTest() {
    ThreadReferencingThreadFactory tf = makeThreadFactory();
    SingleThreadScheduler sts = new SingleThreadScheduler(tf);
    sts.prestartExecutionThread(true);
    
    Thread thread2 = tf.newThread(DoNothingRunnable.instance());
    
    List<Thread> result = tf.getThreads(true);
    assertEquals(1, result.size());
    assertFalse(result.contains(thread2)); // it was never started
  }
}

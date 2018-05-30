package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;
import org.threadly.util.TestExceptionHandler;
import org.threadly.util.TestUncaughtExceptionHandler;

@SuppressWarnings("javadoc")
public class ConfigurableThreadFactoryTest extends ThreadlyTester {
  protected ConfigurableThreadFactory makeThreadFactory() {
    return new ConfigurableThreadFactory();
  }
  
  protected ConfigurableThreadFactory makeThreadFactory(String poolPrefix, boolean appendPoolId) {
    return new ConfigurableThreadFactory(poolPrefix, appendPoolId);
  }
  
  protected ConfigurableThreadFactory makeThreadFactory(boolean daemon) {
    return new ConfigurableThreadFactory(daemon);
  }
  
  protected ConfigurableThreadFactory makeThreadFactory(int threadPriority) {
    return new ConfigurableThreadFactory(threadPriority);
  }
  
  protected ConfigurableThreadFactory makeThreadFactory(UncaughtExceptionHandler ueh) {
    return new ConfigurableThreadFactory(ueh);
  }

  protected ConfigurableThreadFactory makeThreadFactory(ExceptionHandler eh) {
    return new ConfigurableThreadFactory(eh);
  }
  
  @Test
  public void emptyConstructorTest() {
    ThreadFactory defaultFactory = makeThreadFactory();
    ConfigurableThreadFactory ctf = makeThreadFactory();
    
    Thread defaultThread = defaultFactory.newThread(DoNothingRunnable.instance());
    Thread configurableThread = ctf.newThread(DoNothingRunnable.instance());
    
    String defaultName = defaultThread.getName();
    String configurableName = configurableThread.getName();
    int firstDashIndex = defaultName.indexOf('-');
    int secondDashIndex = defaultName.indexOf('-', firstDashIndex + 1);
    String defaultSantaizedName = defaultName.substring(0, firstDashIndex) + defaultName.substring(secondDashIndex);
    firstDashIndex = configurableName.indexOf('-');
    secondDashIndex = configurableName.indexOf('-', firstDashIndex + 1);
    String configurableSantaizedName = configurableName.substring(0, firstDashIndex) + configurableName.substring(secondDashIndex);
    assertEquals(defaultSantaizedName, configurableSantaizedName);
    
    assertEquals(defaultThread.isDaemon(), configurableThread.isDaemon());
    assertEquals(defaultThread.getPriority(), configurableThread.getPriority());
    assertTrue(defaultThread.getUncaughtExceptionHandler() == configurableThread.getUncaughtExceptionHandler());
    assertTrue(defaultThread.getThreadGroup() == configurableThread.getThreadGroup());
    assertFalse(configurableThread.isAlive());
  }
  
  @Test
  public void setPrefixWithPoolIdTest() {
    String poolPrefix = StringUtils.makeRandomString(5);
    ConfigurableThreadFactory ctf1 = makeThreadFactory(poolPrefix, true);
    ConfigurableThreadFactory ctf2 = makeThreadFactory(poolPrefix, true);

    assertTrue(ctf1.threadNamePrefix.contains(poolPrefix));
    assertFalse(ctf1.threadNamePrefix.equals(ctf2.threadNamePrefix));
    
    Thread t = ctf1.newThread(DoNothingRunnable.instance());
    assertTrue(t.getName().contains(ctf1.threadNamePrefix));
  }

  @Test
  public void setPrefixWithoutPoolIdTest() {
    String poolPrefix = StringUtils.makeRandomString(5);
    ConfigurableThreadFactory ctf1 = makeThreadFactory(poolPrefix, false);
    ConfigurableThreadFactory ctf2 = makeThreadFactory(poolPrefix, false);

    assertTrue(ctf1.threadNamePrefix.contains(poolPrefix));
    assertTrue(ctf1.threadNamePrefix.equals(ctf2.threadNamePrefix));
    
    Thread t = ctf1.newThread(DoNothingRunnable.instance());
    assertTrue(t.getName().contains(ctf1.threadNamePrefix));
  }
  
  @Test
  public void useDaemonThreadTest() {
    ConfigurableThreadFactory ctfFalse = makeThreadFactory(false);
    ConfigurableThreadFactory ctfTrue = makeThreadFactory(true);
    
    Thread t;
    assertFalse(ctfFalse.useDaemonThreads);
    t = ctfFalse.newThread(DoNothingRunnable.instance());
    assertFalse(t.isDaemon());
    
    assertTrue(ctfTrue.useDaemonThreads);
    t = ctfTrue.newThread(DoNothingRunnable.instance());
    assertTrue(t.isDaemon());
  }

  @Test
  public void priorityUnderMinTest() {
    ConfigurableThreadFactory ctf = makeThreadFactory(-1000);
    
    assertEquals(Thread.MIN_PRIORITY, ctf.threadPriority);
  }
  
  @Test
  public void priorityOverMaxTest() {
    ConfigurableThreadFactory ctf = makeThreadFactory(1000);
    
    assertEquals(Thread.MAX_PRIORITY, ctf.threadPriority);
  }

  @Test
  public void setPriorityTest() {
    int priority = Thread.NORM_PRIORITY + 1;
    ConfigurableThreadFactory ctf = makeThreadFactory(priority);
    
    assertEquals(priority, ctf.threadPriority);
    Thread t = ctf.newThread(DoNothingRunnable.instance());
    assertEquals(priority, t.getPriority());
  }
  
  @Test
  public void setUncaughtExceptionHandlerTest() {
    TestUncaughtExceptionHandler ueh = new TestUncaughtExceptionHandler();
    ConfigurableThreadFactory ctf = makeThreadFactory(ueh);
    
    assertEquals(ueh, ctf.defaultUncaughtExceptionHandler);
    Thread t = ctf.newThread(DoNothingRunnable.instance());
    assertEquals(ueh, t.getUncaughtExceptionHandler());
  }

  @Test
  public void setExceptionHandlerTest() {
    TestExceptionHandler teh = new TestExceptionHandler();
    ConfigurableThreadFactory ctf = makeThreadFactory(teh);
    
    assertEquals(teh, ctf.defaultThreadlyExceptionHandler);
    
    final AtomicReference<ExceptionHandler> ehi = new AtomicReference<>(null);
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() {
        ehi.set(ExceptionUtils.getExceptionHandler());
      }
    };
    Thread t = ctf.newThread(tr);
    t.start();
    tr.blockTillFinished();
    
    assertTrue(ehi.get() == teh);
  }
}

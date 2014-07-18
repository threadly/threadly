package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ThreadFactory;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ConfigurableThreadFactoryTest {
  @Test
  public void emptyConstructorTest() {
    ThreadFactory defaultFactory = new ConfigurableThreadFactory();
    ConfigurableThreadFactory ctf = new ConfigurableThreadFactory();
    
    Thread defaultThread = defaultFactory.newThread(new TestRunnable());
    Thread configurableThread = ctf.newThread(new TestRunnable());
    
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
    String poolPrefix = "foo";
    ConfigurableThreadFactory ctf1 = new ConfigurableThreadFactory(poolPrefix, true);
    ConfigurableThreadFactory ctf2 = new ConfigurableThreadFactory(poolPrefix, true);

    assertTrue(ctf1.threadNamePrefix.contains(poolPrefix));
    assertFalse(ctf1.threadNamePrefix.equals(ctf2.threadNamePrefix));
    
    Thread t = ctf1.newThread(new TestRunnable());
    assertTrue(t.getName().contains(ctf1.threadNamePrefix));
  }
  
  @Test
  public void setPrefixWithoutPoolIdTest() {
    String poolPrefix = "foo";
    ConfigurableThreadFactory ctf1 = new ConfigurableThreadFactory(poolPrefix, false);
    ConfigurableThreadFactory ctf2 = new ConfigurableThreadFactory(poolPrefix, false);

    assertTrue(ctf1.threadNamePrefix.contains(poolPrefix));
    assertTrue(ctf1.threadNamePrefix.equals(ctf2.threadNamePrefix));
    
    Thread t = ctf1.newThread(new TestRunnable());
    assertTrue(t.getName().contains(ctf1.threadNamePrefix));
  }
  
  @Test
  public void useDaemonThreadTest() {
    ConfigurableThreadFactory ctfFalse = new ConfigurableThreadFactory(false);
    ConfigurableThreadFactory ctfTrue = new ConfigurableThreadFactory(true);
    
    Thread t;
    assertFalse(ctfFalse.useDaemonThreads);
    t = ctfFalse.newThread(new TestRunnable());
    assertFalse(t.isDaemon());
    
    assertTrue(ctfTrue.useDaemonThreads);
    t = ctfTrue.newThread(new TestRunnable());
    assertTrue(t.isDaemon());
  }
  
  @Test
  public void priorityUnderMinTest() {
    ConfigurableThreadFactory ctf = new ConfigurableThreadFactory(-1000);
    
    assertEquals(Thread.MIN_PRIORITY, ctf.threadPriority);
  }
  
  @Test
  public void priorityOverMaxTest() {
    ConfigurableThreadFactory ctf = new ConfigurableThreadFactory(1000);
    
    assertEquals(Thread.MAX_PRIORITY, ctf.threadPriority);
  }
  
  @Test
  public void setPriorityTest() {
    int priority = Thread.NORM_PRIORITY + 1;
    ConfigurableThreadFactory ctf = new ConfigurableThreadFactory(priority);
    
    assertEquals(priority, ctf.threadPriority);
    Thread t = ctf.newThread(new TestRunnable());
    assertEquals(priority, t.getPriority());
  }
  
  @Test
  public void setUncaughtExceptionHandlerTest() {
    TestUncaughtExceptionHandler ueh = new TestUncaughtExceptionHandler();
    ConfigurableThreadFactory ctf = new ConfigurableThreadFactory(ueh);
    
    assertEquals(ueh, ctf.defaultUncaughtExceptionHandler);
    Thread t = ctf.newThread(new TestRunnable());
    assertEquals(ueh, t.getUncaughtExceptionHandler());
  }
}

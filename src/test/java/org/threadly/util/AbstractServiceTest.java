package org.threadly.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class AbstractServiceTest extends ThreadlyTester {
  private TestService service;
  
  @BeforeEach
  public void setup() {
    service = new TestService();
  }
  
  @AfterEach
  public void cleanup() {
    service.stopIfRunning();  // prevent GC warning
    service = null;
  }
  
  @Test
  public void startAndIsRunningTest() {
    assertFalse(service.isRunning());
    
    service.start();
    
    assertTrue(service.isRunning());
  }
  
  @Test
  public void hasStoppedTest() {
    assertFalse(service.hasStopped());
    service.start();
    assertFalse(service.hasStopped());
    
    service.stop();
    assertTrue(service.hasStopped());
  }
  
  @Test
  public void startFail() {
      assertThrows(IllegalStateException.class, () -> {
      service.start();
      service.start();
      });
  }
  
  @Test
  public void startIfNotStartedTest() {
    assertFalse(service.isRunning());
    
    assertTrue(service.startIfNotStarted());
    
    assertTrue(service.isRunning());
    
    assertFalse(service.startIfNotStarted());
    
    assertTrue(service.isRunning());
  }
  
  @Test
  public void stopTest() {
    assertFalse(service.isRunning());
    service.start();
    assertTrue(service.isRunning());
    
    service.stop();
    
    assertFalse(service.isRunning());
  }
  
  @Test
  public void stopNotStartedFail() {
      assertThrows(IllegalStateException.class, () -> {
      service.stop();
      });
  }
  
  @Test
  public void stopTwiceFail() {
      assertThrows(IllegalStateException.class, () -> {
      service.start();
    
      service.stop();
      service.stop();
      });
  }
  
  @Test
  public void stopIfRunningTest() {
    assertFalse(service.isRunning());
    assertFalse(service.stopIfRunning());
    
    service.start();
    assertTrue(service.isRunning());
    
    assertTrue(service.stopIfRunning());
    assertFalse(service.isRunning());
    
    assertFalse(service.stopIfRunning());
  }
  
  private static class TestService extends AbstractService {
    private final AtomicBoolean startCalled = new AtomicBoolean(false);
    private final AtomicBoolean stopCalled = new AtomicBoolean(false);

    @Override
    protected void startupService() {
      if (! startCalled.compareAndSet(false, true)) {
        throw new StackSuppressedRuntimeException();
      }
    }

    @Override
    protected void shutdownService() {
      if (! stopCalled.compareAndSet(false, true)) {
        throw new StackSuppressedRuntimeException();
      }
    }
  }
}

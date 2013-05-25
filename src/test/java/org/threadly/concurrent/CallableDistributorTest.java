package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class CallableDistributorTest {
  private PriorityScheduledExecutor executor;
  private CallableDistributor<String, String> distributor;
  
  @Before
  public void setup() {
    executor = new PriorityScheduledExecutor(10, 10, 200);
    distributor = new CallableDistributor<String, String>(executor);
  }
  
  @After
  public void tearDown() {
    executor.shutdown();
    executor = null;
    distributor = null;
  }
  
  /*@Test
  public void getNextResultTest() {
    // TODO - implement
  }
  
  @Test
  public void getAllResultsTest() {
    // TODO - implement
  }*/
}

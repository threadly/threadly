package org.threadly.concurrent.statistic;

import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

// this class is used because it would be most likely to expose a case where a longer delay than actual is reported
class ClockUpdateRunnable extends TestRunnable {
  public ClockUpdateRunnable() {
    super();
  }
  
  public ClockUpdateRunnable(int runTime) {
    super(runTime);
  }
  
  @Override
  public void handleRunFinish() {
    Clock.accurateTimeNanos();
  }
}
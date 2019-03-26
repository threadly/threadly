package org.threadly.util.debug;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 * This class functions very similar to the {@link Profiler}.  The difference between the two is
 * that this class only counts samples whose stack trace contains an entry matching a particular
 * pattern.
 *
 * <p>This is useful for drilling down into particular regions of code within an application also
 * busy doing other things; for exmaple, a production HTTP API may have many endpoints, but you
 * only want to know what one of them spends its time doing.
 *
 * @since 5.35.0
 */
public class FilteredStackProfiler extends Profiler {
  protected final FilteredStackProfileStorage filteredThreadStore;

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pattern Only stack traces where the string representation of a
   * {@link StackTraceElement} matches this regular expression will be counted.
   */
  public FilteredStackProfiler(String pattern) {
    this(Pattern.compile(pattern));
  }

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pattern Only stack traces where the string representation of a
   * {@link StackTraceElement} matches this regular expression will be counted.
   */
  public FilteredStackProfiler(Pattern pattern) {
    this(DEFAULT_POLL_INTERVAL_IN_MILLIS, pattern);
  }

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pollIntervalInMs frequency to check running threads
   * @param pattern Only stack traces where the string representation of a
   * {@link StackTraceElement} matches this regular expression will be counted.
   */
  public FilteredStackProfiler(int pollIntervalInMs, String pattern) {
    this(pollIntervalInMs, Pattern.compile(pattern));
  }

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pollIntervalInMs frequency to check running threads
   * @param pattern Only stack traces where the string representation of a
   * {@link StackTraceElement} matches this regular expression will be counted.
   */
  public FilteredStackProfiler(int pollIntervalInMs, Pattern pattern) {
    super(new FilteredStackProfileStorage(pollIntervalInMs, pattern));

    this.filteredThreadStore = (FilteredStackProfileStorage)super.pStore;
  }

  /**
   * Extending class of {@link ProfileStorage} this overrides
   * {@link #getProfileThreadsIterator()}.  It controls it so that only samples which match the
   * desired pattern are returned to the profiler.
   *
   * @since 3.35.0
   */
  protected static class FilteredStackProfileStorage extends ProfileStorage {
    protected final Pattern pattern;

    public FilteredStackProfileStorage(int pollIntervalInMs, Pattern pattern) {
      super(pollIntervalInMs);

      if (null == pattern) {
        throw new NullPointerException("pattern");
      }

      this.pattern = pattern;
    }

    @Override
    protected Iterator<? extends ThreadSample> getProfileThreadsIterator() {
      return new FilteredStackSampleIterator(super.getProfileThreadsIterator(), pattern);
    }
  }

  /**
   * Adapts a {@code ThreadSample} iterator to only return samples matching the desired pattern.
   */
  private static class FilteredStackSampleIterator implements Iterator<ThreadSample> {
    private final Iterator<? extends ThreadSample> delegate;
    private final Pattern pattern;
    private ThreadSample next;

    FilteredStackSampleIterator(
      Iterator<? extends ThreadSample> delegate,
      Pattern pattern
    ) {
      this.delegate = delegate;
      this.pattern = pattern;

      findNext();
    }

    @Override
    public boolean hasNext() {
      return null != next;
    }

    @Override
    public ThreadSample next() {
      if (null == next) {
        throw new NoSuchElementException();
      }

      ThreadSample toReturn = next;
      findNext();
      return toReturn;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private void findNext() {
      while (delegate.hasNext()) {
        // We need to cache the stack trace so that it doesn't change between filtering it and
        // recording it in the profiler.
        next = new CachedThreadSample(delegate.next());
        for (StackTraceElement element: next.getStackTrace()) {
          if (pattern.matcher(element.toString()).find()) {
            return;
          }
        }
      }

      next = null;
    }
  }

  /**
   * A {@code ThreadSample} with a precalculated stack trace.
   *
   * <p>This is used internally so that the stack trace is the same when we apply the filter and
   * then later add it to the profile.
   */
  private static class CachedThreadSample implements ThreadSample {
    private final Thread thread;
    private final StackTraceElement[] stackTrace;

    CachedThreadSample(ThreadSample orig) {
      this.thread = orig.getThread();
      this.stackTrace = orig.getStackTrace();
    }

    @Override
    public Thread getThread() {
      return thread;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
      return stackTrace;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof ThreadSample) {
        return ((ThreadSample)o).getThread() == thread;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return thread.hashCode();
    }
  }
}

package org.threadly.util.debug;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * This class functions very similar to the {@link Profiler}.  The difference between the two is
 * that this class only counts samples whose stack trace contains an entry matching a particular
 * pattern.
 * <p>
 * This is useful for drilling down into particular regions of code within an application also
 * busy doing other things; for example, a production HTTP API may have many endpoints, but you
 * only want to know what one of them spends its time doing.
 *
 * @since 5.35
 */
public class FilteredStackProfiler extends Profiler {
  protected final FilteredStackProfileStorage filteredThreadStore;

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pattern Only stack traces where the string representation of a
   *                  {@link StackTraceElement} matches this regular expression will be counted.
   */
  public FilteredStackProfiler(String pattern) {
    this(regexPredicate(pattern));
  }

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param filter Only stack traces where the predicate returns {@code true} will be included
   */
  public FilteredStackProfiler(Predicate<StackTraceElement[]> filter) {
    this(DEFAULT_POLL_INTERVAL_IN_MILLIS, filter);
  }

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pollIntervalInMs frequency to check running threads
   * @param pattern Only stack traces where the string representation of a
   *                  {@link StackTraceElement} matches this regular expression will be counted.
   */
  public FilteredStackProfiler(int pollIntervalInMs, String pattern) {
    this(pollIntervalInMs, regexPredicate(pattern));
  }

  /**
   * Constructs a new profiler instance.  The only way to get results from this instance is to
   * call {@code #dump()} with a provided output stream to get the results to.
   *
   * @param pollIntervalInMs frequency to check running threads
   * @param filter Only stack traces where the predicate returns {@code true} will be included
   */
  public FilteredStackProfiler(int pollIntervalInMs, Predicate<StackTraceElement[]> filter) {
    super(new FilteredStackProfileStorage(pollIntervalInMs, filter));

    this.filteredThreadStore = (FilteredStackProfileStorage)super.pStore;
  }

  private static Predicate<StackTraceElement[]> regexPredicate(String pattern) {
    final Pattern compiled = Pattern.compile(pattern);
    return (stack) -> {
      for (StackTraceElement element : stack) {
        if (compiled.matcher(element.toString()).find()) {
          return true;
        }
      }
      return false;
    };
  }

  /**
   * Extending class of {@link ProfileStorage} this overrides
   * {@link #getProfileThreadsIterator()}.  It controls it so that only samples which match the
   * desired pattern are returned to the profiler.
   *
   * @since 3.35
   */
  protected static class FilteredStackProfileStorage extends ProfileStorage {
    protected final Predicate<StackTraceElement[]> filter;

    public FilteredStackProfileStorage(int pollIntervalInMs, Predicate<StackTraceElement[]> filter) {
      super(pollIntervalInMs);
      
      ArgumentVerifier.assertNotNull(filter, "filter");

      this.filter = filter;
    }

    @Override
    protected Iterator<? extends ThreadSample> getProfileThreadsIterator() {
      return new FilteredStackSampleIterator(super.getProfileThreadsIterator(), filter);
    }
  }

  /**
   * Adapts a {@code ThreadSample} iterator to only return samples matching the desired pattern.
   * 
   * @since 3.35
   */
  private static class FilteredStackSampleIterator implements Iterator<ThreadSample> {
    private final Iterator<? extends ThreadSample> delegate;
    private final Predicate<StackTraceElement[]> filter;
    private ThreadSample next;

    FilteredStackSampleIterator(Iterator<? extends ThreadSample> delegate,
                                Predicate<StackTraceElement[]> filter) {
      this.delegate = delegate;
      this.filter = filter;

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
        next = delegate.next();

        try {
          if (filter.test(next.getStackTrace())) {
            return;
          }
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
          return; // Be conservative and include the data
        }
      }

      next = null;
    }
  }
}

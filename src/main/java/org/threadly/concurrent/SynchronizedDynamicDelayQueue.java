package org.threadly.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.RandomAccess;
import java.util.logging.Logger;
import java.util.NoSuchElementException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.util.ListUtils;

/**
 * This is a DynamicDelayQueue that is thread safe by strict
 * usage of synchronization.
 * 
 * @author jent - Mike Jensen
 * @param <T> Parameter to indicate what type of item is contained in the queue
 */
public class SynchronizedDynamicDelayQueue<T extends Delayed> implements DynamicDelayQueue<T> {
  private static final Logger log = Logger.getLogger(SynchronizedDynamicDelayQueue.class.getSimpleName());
  private static final boolean VERBOSE = false;

  private final LinkedList<T> queue; 
  private final boolean randomAccessQueue;
  private final VirtualLock queueLock;
  
  /**
   * Constructs a new queue
   */
  public SynchronizedDynamicDelayQueue() {
    this(new NativeLock());
  }
  
  /**
   * Constructs a queue, providing the lock that will be called 
   * on with .await().  Thus it allows you to synchronize around
   * the .take() and have the lock released while the thread blocks.
   * 
   * @param queueLock lock that is used internally
   */
  public SynchronizedDynamicDelayQueue(VirtualLock queueLock) {
    if (queueLock == null) {
      throw new IllegalArgumentException("Must provided queue lock");
    }
    
    queue = new LinkedList<T>();
    randomAccessQueue = ((Object)queue instanceof RandomAccess);
    this.queueLock = queueLock;
  }

  @Override
  public Object getLock() {
    return queueLock;
  }
  
  @Override
  public void sortQueue() {
    synchronized (queueLock) {
      Collections.sort(queue);
      
      queueLock.signalAll();
    }
  }

  @Override
  public boolean add(T e) {
    if (e == null) {
      return false;
    }
    
    synchronized (queueLock) {
      int insertionIndex = ListUtils.getInsertionEndIndex(queue, e, randomAccessQueue);
      
      queue.add(insertionIndex, e);
      
      queueLock.signalAll();
    }
    
    return true;
  }
  
  @Override
  public void reposition(T e) {
    if (e == null) {
      return;
    }
    
    synchronized (queueLock) {
      boolean found = false;
      // we search backwards because the most common case will be items at the end that need to be repositioned forward
      ListIterator<T> it = queue.listIterator(queue.size());
      while (it.hasPrevious()) {
        if (it.previous().equals(e)) {
          it.remove();
          found = true;
        }
      }
      
      if (! found) {
        throw new IllegalStateException("Could not find item: " + e);
      } else {
        add(e);
      }
    }
  }
  
  @Override
  public void addLast(T e) {
    if (e == null) {
      throw new NullPointerException();
    }
    
    synchronized (queueLock) {
      queue.add(e);
    }
  }

  @Override
  public T element() {
    T result = peek();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public boolean offer(T e) {
    return add(e);  // LinkedList has no space limit
  }

  @Override
  public T peek() {
    T next = null;
    synchronized (queueLock) {
      if (! queue.isEmpty()) {
        next = queue.getFirst();
      }
      if (next != null && next.getDelay(TimeUnit.MILLISECONDS) > 0) {
        next = null;
      }
    }
    
    return next;
  }

  @Override
  public T poll() {
    T next = null;
    synchronized (queueLock) {
      if (! queue.isEmpty() && 
          queue.getFirst().getDelay(TimeUnit.MILLISECONDS) <= 0) {
        next = queue.removeFirst();
      }
    }
    
    return next;
  }
  
  protected void blockTillAvailable() throws InterruptedException {
    synchronized (queueLock) {
      while (queue.isEmpty()) {
        if (VERBOSE) {
          log.info("Queue is empty, waiting");
        }

        queueLock.await();
      }
      long nextDelay = -1;
      while ((nextDelay = queue.getFirst().getDelay(TimeUnit.MILLISECONDS)) > 0) {
        if (nextDelay > 5) {  // spin lock if delay is < 5
          if (VERBOSE) {
            log.info("Next items delay is: " + nextDelay + 
                       "...waiting for: " + queue.getFirst());
          }

          queueLock.await(nextDelay);
        }
      }
    }
  }
  
  @Override
  public T take() throws InterruptedException {
    T next = null;
    synchronized (queueLock) {
      blockTillAvailable();
      
      next = queue.removeFirst();
    }
   
    if (VERBOSE) {
      log.info("Returning next item: " + next + 
                 ", delay is: " + next.getDelay(TimeUnit.MILLISECONDS));
    }
    return next;
  }

  @Override
  public T remove() {
    T result = poll();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    synchronized (queueLock) {
      Iterator<? extends T> it = c.iterator();
      boolean added = it.hasNext();
      while (it.hasNext()) {
        add(it.next());
      }
      
      return added;
    }
  }

  @Override
  public void clear() {
    synchronized (queueLock) {
      queue.clear();
    }
  }

  @Override
  public boolean contains(Object o) {
    synchronized (queueLock) {
      return queue.contains(o);
    }
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    synchronized (queueLock) {
      return queue.containsAll(c);
    }
  }

  @Override
  public boolean isEmpty() {
    synchronized (queueLock) {
      return queue.isEmpty();
    }
  }

  @Override
  public Iterator<T> iterator() {
    if (! Thread.holdsLock(queueLock)) {
      throw new IllegalStateException("Must have lock in order to get iterator");
    }
    
    return queue.iterator();
  }
  
  @Override
  public ConsumerIterator<T> consumeIterator() throws InterruptedException {
    if (! Thread.holdsLock(queueLock)) {
      throw new IllegalStateException("Must have lock in order to get iterator");
    }
    
    blockTillAvailable();
    
    return new ConsumerIterator<T>() {
      private T next = null;
      
      @Override
      public boolean hasNext() {
        if (next == null) {
          next = SynchronizedDynamicDelayQueue.this.peek();
        }
        
        return next != null;
      }
      
      @Override
      public T peek() {
        if (next == null) {
          next = SynchronizedDynamicDelayQueue.this.peek();
        }
        
        return next;
      }

      @Override
      public T remove() {
        T result;
        if (next != null) {
          result = next;
          queue.remove(result);
          next = null;
        } else {
          result = SynchronizedDynamicDelayQueue.this.remove();
        }
        
        return result;
      }
    };
  }

  @Override
  public boolean remove(Object o) {
    synchronized (queueLock) {
      return queue.remove(o);
    }
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    synchronized (queueLock) {
      return queue.removeAll(c);
    }
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    synchronized (queueLock) {
      return queue.retainAll(c);
    }
  }

  @Override
  public int size() {
    synchronized (queueLock) {
      return queue.size();
    }
  }

  @Override
  public Object[] toArray() {
    synchronized (queueLock) {
      return queue.toArray();
    }
  }

  @Override
  public <E> E[] toArray(E[] a) {
    synchronized (queueLock) {
      return queue.toArray(a);
    }
  }
}

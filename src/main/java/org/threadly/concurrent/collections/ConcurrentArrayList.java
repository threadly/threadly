package org.threadly.concurrent.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>A thread safe list implementation with an array back end.  Make sure to read the javadocs 
 * carefully, as several functions behave subtly different from the java.util.List definition.</p>
 * 
 * <p>The design of this implementation is NOT to completely avoid synchronization.  We have a 
 * hybrid implementation of volatile and synchronized to allow for cheaper reading, but keeping 
 * high consistency.  It works with the idea that the internal data is immutable.  Each read has 
 * an immutable version of the data.  Thus making writes more expensive (almost like a 
 * CopyOnWriteArrayList).</p>
 * 
 * <p>There are several differences between this and a CopyOnWriteArrayList.  The first being that 
 * we don't have to copy the structure on every write operation.  By setting the front and/or rear 
 * padding, we can add items to the front or end of the list while avoiding a copy operation.  In 
 * addition removals also do not require a copy operation.  Furthermore, this implementation 
 * differs from  a CopyOnWriteArrayList is that it does allow some synchronization.  Which can 
 * give higher consistency guarantees for some operations by allowing you to synchronize on the 
 * modification lock to perform multiple atomic operations.</p>
 * 
 * <p>The main motivation in implementing this was to avoid array copies as much as possible 
 * (which for large lists can be costly).  But also the implementation to cheaply reposition an 
 * item in the list was necessary for other performance benefits.</p>
 * 
 * <p>A couple notable points is that subList calls are very cheap, but modifications to sublist 
 * are completely independent from their source list.</p>
 * 
 * <p>Unlike CopyOnWriteArrayList, Iterators can attempt to modify the state of the backing 
 * structure (assuming it still makes sense to do so).  Although unlike CopyOnWriteArrayList 
 * iterators, once an Iterator is created it will never see updates to the structure.  For that 
 * reason it is impossible to have a ConcurrentModificationExcception.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <T> type of object to retain
 */
public class ConcurrentArrayList<T> implements List<T>, Deque<T>, RandomAccess {
  private static final int HASH_CODE_PRIME_NUMBER = 31;
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
  
  protected static <E> DataSet<E> makeEmptyDataSet(int frontPadding, int rearPadding) {
    ArgumentVerifier.assertNotNegative(frontPadding, "frontPadding");
    ArgumentVerifier.assertNotNegative(rearPadding, "rearPadding");
    
    return new DataSet<E>(EMPTY_OBJECT_ARRAY, 0, 0, frontPadding, rearPadding);
  }
  
  protected final Object modificationLock;
  protected volatile DataSet<T> currentData;
  
  /**
   * Constructs a new {@link ConcurrentArrayList} with a new internal NativeLock implementation.
   */
  public ConcurrentArrayList() {
    this(0, 0);
  }
  
  /**
   * Constructs a new {@link ConcurrentArrayList} with specific padding.  Specifying the padding 
   * amounts can optimize this implementation more for the specific use case.  If there is space 
   * in the array for adds to the front or end, then we are able to avoid an array copy.
   * 
   * @param frontPadding padding to add to front of array to possible avoid array copies
   * @param rearPadding padding to add to end of array to possible avoid array copies
   */
  public ConcurrentArrayList(int frontPadding, int rearPadding) {
    this(null, frontPadding, rearPadding);
  }

  /**
   * Constructs a new {@link ConcurrentArrayList} with a provided lock object.  This is the lock 
   * used to guard modifications, and is returned from {@link #getModificationLock()}.
   * 
   * @param modificationLock lock to synchronize on internally
   */
  protected ConcurrentArrayList(Object modificationLock) {
    this(modificationLock, 0, 0);
  }

  /**
   * Constructs a new {@link ConcurrentArrayList} with a provided lock object.  This is the lock 
   * used to guard modifications, and is returned from {@link #getModificationLock()}.  Specifying 
   * the padding amounts can optimize this implementation more for the specific use case.  If 
   * there is space in the array for adds to the front or end, then we are able to avoid an array 
   * copy.
   * 
   * @param modificationLock lock to synchronize on internally
   * @param frontPadding padding to add to front of array to possible avoid array copies
   * @param rearPadding padding to add to end of array to possible avoid array copies
   */
  protected ConcurrentArrayList(Object modificationLock, 
                                int frontPadding, int rearPadding) {
    this(ConcurrentArrayList.<T>makeEmptyDataSet(frontPadding, 
                                                 rearPadding), 
         modificationLock);
  }
  
  /**
   * Internal constructor which provides the modification lock and the initial {@link DataSet}.  
   * This is used for constructing sub-lists, but may also be useful to extending classes.
   * 
   * @param startSet {@link DataSet} to use internally
   * @param modificationLock lock to synchronize on internally
   */
  protected ConcurrentArrayList(DataSet<T> startSet, Object modificationLock) {
    ArgumentVerifier.assertNotNull(startSet, "startSet");
    if (modificationLock == null) {
      modificationLock = new Object();
    }
    
    this.modificationLock = modificationLock;
    currentData = startSet;
  }
  
  /**
   * If you want to chain multiple calls together and ensure that no threads modify the structure 
   * during that time you can get the lock to prevent additional modifications.  
   * 
   * This lock should be synchronized on to prevent modifications.
   * 
   * @return lock used internally
   */
  public Object getModificationLock() {
    return modificationLock;
  }
  
  /**
   * This changes the configuration for the front padding amount for future modification 
   * operations.
   * 
   * @param frontPadding New value to over allocate the front of new buffers
   */
  public void setFrontPadding(int frontPadding) {
    ArgumentVerifier.assertNotNegative(frontPadding, "frontPadding");
    
    synchronized (modificationLock) {
      currentData.frontPadding = frontPadding;
    }
  }

  /**
   * This changes the configuration for the rear padding amount for future modification operations.
   * 
   * @param rearPadding New value to over allocate the rear of new buffers
   */
  public void setRearPadding(int rearPadding) {
    ArgumentVerifier.assertNotNegative(rearPadding, "rearPadding");

    synchronized (modificationLock) {
      currentData.rearPadding = rearPadding;
    }
  }
  
  /**
   * Getter for current amount to added padding to the front of new buffers.
   * 
   * @return current amount to added padding to the front of new buffers
   */
  public int getFrontPadding() {
    return currentData.frontPadding;
  }

  /**
   * Getter for current amount to added padding to the rear of new buffers.
   * 
   * @return current amount to added padding to the rear of new buffers
   */
  public int getRearPadding() {
    return currentData.rearPadding;
  }

  /**
   * Trims the internally array to discard any unused storage.  It is good to invoke this if 
   * future adds are unlikely, and it is desired to keep memory usage minimal.  This does not 
   * adjust the set front or rear padding, so additional modifications will expand the array based 
   * off those set values.  To make sure additional modifications do not expand the array any more 
   * than necessary invoke {@link #setFrontPadding(int)} and {@link #setRearPadding(int)} with 
   * {@code 0}.
   */
  public void trimToSize() {
    synchronized (modificationLock) {
      currentData = currentData.trimToSize();
    }
  }

  @Override
  public int size() {
    return currentData.size;
  }

  @Override
  public boolean isEmpty() {
    return currentData.size == 0;
  }
  
  @Override
  public T get(int index) {
    try {
      return currentData.get(index);
    } catch (ArrayIndexOutOfBoundsException e) {
      // translate to the expected exception type
      throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public int indexOf(Object o) {
    return currentData.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return currentData.lastIndexOf(o);
  }

  @Override
  public boolean contains(Object o) {
    return currentData.indexOf(o) >= 0;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    if (c == null || c.isEmpty()) {
      return true;
    }
    
    DataSet<T> workingSet = currentData;
    Iterator<?> it = c.iterator();
    while (it.hasNext()) {
      if (workingSet.indexOf(it.next()) < 0) {
        return false;
      }
    }
    
    return true;
  }

  @Override
  public Object[] toArray() {
    DataSet<T> workingSet = currentData;

    return Arrays.copyOfRange(workingSet.dataArray, 
                              workingSet.dataStartIndex, 
                              workingSet.dataEndIndex);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> E[] toArray(E[] a) {
    DataSet<T> workingSet = currentData;
    
    if (a.length < workingSet.size) {
      return (E[])Arrays.copyOfRange(workingSet.dataArray, 
                                     workingSet.dataStartIndex, 
                                     workingSet.dataEndIndex, 
                                     a.getClass());
    } else {
      System.arraycopy(workingSet.dataArray, workingSet.dataStartIndex, 
                       a, 0, workingSet.size);
      
      return a;
    }
  }

  @Override
  public boolean add(T e) {
    if (e == null) {
      return false;
    }
    
    synchronized (modificationLock) {
      currentData = currentData.addToEnd(e);
    }
    
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    if (c == null) {
      return false;
    }
    
    Iterator<? extends T> it = c.iterator();
    while (it.hasNext()) {
      if (it.next() == null) {
        it.remove();
      }
    }
    if (c.isEmpty()) {
      return false;
    }

    synchronized (modificationLock) {
      currentData = currentData.addAll(c);
    }
    
    return true;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("Index can not be negative");
    } else if (c == null) {
      return false;
    }
    
    Iterator<? extends T> it = c.iterator();
    while (it.hasNext()) {
      if (it.next() == null) {
        it.remove();
      }
    }
    if (c.isEmpty()) {
      return false;
    }

    synchronized (modificationLock) {
      if (index > currentData.size) {
        throw new IndexOutOfBoundsException("Index is beyond the array size: " + index);
      }
      
      currentData = currentData.addAll(index, c);
    }
    
    return true;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    if (c == this) {
      return false;
    } else if (c == null || c.isEmpty()) {
      if (isEmpty()) {
        return false;
      } else {
        clear();
        
        return true;
      }
    }
    
    DataSet<T> originalSet;
    DataSet<T> resultSet;
    synchronized (modificationLock) {
      originalSet = currentData;
      currentData = resultSet = currentData.retainAll(c);
    }
    
    return ! resultSet.equalsExactly(originalSet);
  }

  @Override
  public void clear() {
    synchronized (modificationLock) {
      currentData = makeEmptyDataSet(currentData.frontPadding, 
                                     currentData.rearPadding);
    }
  }
  
  @Override
  public void addFirst(T e) {
    // nulls can't be accepted because of how we attempt to prevent array copies
    if (e == null) {
      throw new UnsupportedOperationException("This structure can not accept nulls");
    }
    
    synchronized (modificationLock) {
      currentData = currentData.addToFront(e);
    }
  }

  @Override
  public void addLast(T e) {
    // nulls can't be accepted because of how we attempt to prevent array copies
    if (e == null) {
      throw new UnsupportedOperationException("This structure can not accept nulls");
    }
    
    synchronized (modificationLock) {
      currentData = currentData.addToEnd(e);
    }
  }

  @Override
  public boolean offerFirst(T e) {
    addFirst(e);
      
    // this implementation has no capacity limit
    return true;
  }

  @Override
  public boolean offerLast(T e) {
    addLast(e);
      
    // this implementation has no capacity limit
    return true;
  }

  @Override
  public T removeFirst() {
    T result = pollFirst();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public T removeLast() {
    T result = pollLast();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public T pollFirst() {
    synchronized (modificationLock) {
      T result = peekFirst();
      if (result != null) {
        currentData = currentData.remove(0);
      }
      
      return result;
    }
  }

  @Override
  public T pollLast() {
    synchronized (modificationLock) {
      T result = peekLast();
      if (result != null) {
        currentData = currentData.remove(currentData.size - 1);
      }
      
      return result;
    }
  }

  @Override
  public T getFirst() {
    T result = peekFirst();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public T getLast() {
    T result = peekLast();
    if (result == null) {
      throw new NoSuchElementException();
    }
    
    return result;
  }

  @Override
  public T peek() {
    return peekFirst();
  }

  @Override
  public T peekFirst() {
    DataSet<T> set = currentData;
    if (set.size > 0) {
      return set.get(0);
    } else {
      return null;
    }
  }

  @Override
  public T peekLast() {
    DataSet<T> set = currentData;
    if (set.size > 0) {
      return set.get(set.size - 1);
    } else {
      return null;
    }
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }

    DataSet<T> originalSet;
    DataSet<T> resultSet;
    synchronized (modificationLock) {
      originalSet = currentData;
      currentData = resultSet = currentData.removeAll(c);
    }
    
    return ! resultSet.equalsExactly(originalSet);
  }

  protected boolean remove(Object o, boolean searchBackwards) {
    if (o == null) {
      return false;
    }
    
    synchronized (modificationLock) {
      int index;
      if (searchBackwards) {
        index = currentData.lastIndexOf(o);
      } else {
        index = currentData.indexOf(o);
      }
      if (index < 0) {
        return false;
      } else {
        currentData = currentData.remove(index);
        return true;
      }
    }
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    return remove(o, false);
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    return remove(o, true);
  }

  @Override
  public boolean remove(Object o) {
    return removeFirstOccurrence(o);
  }

  @Override
  public T remove(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("Index can not be negative");
    }
    
    DataSet<T> originalSet;
    synchronized (modificationLock) {
      if (index > currentData.size - 1) {
        throw new IndexOutOfBoundsException("Index is beyond the array max index: " + index);
      }
      
      originalSet = currentData;
      currentData = currentData.remove(index);
    }
    
    return originalSet.get(index);
  }

  @Override
  public boolean offer(T e) {
    return offerLast(e);
  }

  @Override
  public T remove() {
    return removeFirst();
  }

  @Override
  public T poll() {
    return pollFirst();
  }

  @Override
  public T element() {
    return getFirst();
  }

  @Override
  public void push(T e) {
    addFirst(e);
  }

  @Override
  public T pop() {
    return removeFirst();
  }

  @Override
  public T set(int index, T element) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("Index can not be negative");
    }
    
    DataSet<T> originalSet;
    synchronized (modificationLock) {
      if (index > currentData.size - 1) {
        throw new IndexOutOfBoundsException("Index is beyond the array max index: " + index);
      }
      
      originalSet = currentData;
      currentData = currentData.set(index, element);
    }
    
    return originalSet.get(index);
  }

  @Override
  public void add(int index, T element) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("Index can not be negative");
    }
    
    synchronized (modificationLock) {
      if (index > currentData.size) {
        throw new IndexOutOfBoundsException("Index is beyond the array size: " + index);
      }
      
      currentData = currentData.add(index, element);
    }
  }
  
  /**
   * Move a stored item to a new index.  By default 
   * a forward search will happen to find the item.
   * 
   * @param item item to be moved
   * @param newIndex new index for placement
   */
  public void reposition(T item, int newIndex) {
    reposition(item, newIndex, false);
  }
  
  /**
   * Move a stored item to a new index.  If you have
   * an idea if it is closer to the start or end of the list
   * you can specify which end to start the search on.
   * 
   * @param item item to be moved
   * @param newIndex new index for placement
   * @param searchBackwards true to start from the end and search backwards
   */
  public void reposition(T item, int newIndex, boolean searchBackwards) {
    if (newIndex < 0) {
      throw new IndexOutOfBoundsException("New index can not be negative");
    }
    
    synchronized (modificationLock) {
      if (newIndex > currentData.size) {
        throw new IndexOutOfBoundsException(newIndex + " is beyond the array's size: " + 
                                              currentData.size);
      }
      
      int index;
      if (searchBackwards) {
        index = lastIndexOf(item);
      } else {
        index = indexOf(item);
      }
      if (index < 0) {
        throw new NoSuchElementException("Could not find item: " + item);
      } else if (index == newIndex) {
        return;
      }

      currentData = currentData.reposition(index, newIndex);
    }
  }
  
  /**
   * Move a stored item located at an index to a new index.  Provide 
   * the size for newIndex to move the item to the end of the list.  
   * Otherwise all items after the new index will be shifted right.
   * 
   * @param originalIndex index for item to be moved to.
   * @param newIndex new index location for item.
   */
  public void reposition(int originalIndex, int newIndex) {
    if (newIndex < 0) {
      throw new IndexOutOfBoundsException("new index can not be negative");
    } else if (originalIndex < 0) {
      throw new IndexOutOfBoundsException("original index can not be negative");
    }
    
    if (originalIndex == newIndex) {
      return;
    }
    
    synchronized (modificationLock) {
      if (newIndex > currentData.size) {
        throw new IndexOutOfBoundsException("new index " + newIndex + 
                                              " is beyond the array's length: " + currentData.size);
      } else if (originalIndex > currentData.size) {
        throw new IndexOutOfBoundsException("original index " + originalIndex + 
                                              " is beyond the array's length: " + currentData.size);
      }
      
      currentData = currentData.reposition(originalIndex, newIndex);
    }
  }

  @Override
  public Iterator<T> iterator() {
    return listIterator();
  }

  @Override
  public ListIterator<T> listIterator() {
    return listIterator(0);
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return new DataSetListIterator(currentData, index);
  }

  @Override
  public Iterator<T> descendingIterator() {
    final ListIterator<T> li = listIterator(currentData.size);
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return li.hasPrevious();
      }

      @Override
      public T next() {
        return li.previous();
      }

      @Override
      public void remove() {
        li.remove();
      }
    };
  }

  /**
   * This returns a sub list from the current list.  The initial call
   * is very cheap because it uses the current data backing to produce 
   * (no copying necessary).
   * 
   * But any modifications to this list will be treated as a completely 
   * new list, and wont ever reflect on the source list.  This is very 
   * different from other java.util.List implementations, and should be 
   * noted carefully.
   * 
   * @param fromIndex start index (inclusive) for new list to include
   * @param toIndex end index (exclusive) to be included in new list
   * @return new independent list
   */
  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    DataSet<T> workingData = currentData;
    
    if (fromIndex < 0) {
      throw new IndexOutOfBoundsException("from index can not be negative");
    } else if (fromIndex > workingData.size) {
      throw new IndexOutOfBoundsException("from index must be <= size: " + workingData.size);
    } else if (toIndex > workingData.size) {
      throw new IndexOutOfBoundsException("to index must be <= size: " + workingData.size);
    } else if (toIndex <= fromIndex) {
      throw new IndexOutOfBoundsException("fromIndex must be < toIndex");
    }
    
    DataSet<T> newSet = new DataSet<T>(workingData.dataArray, 
                                       workingData.dataStartIndex + fromIndex, 
                                       workingData.dataEndIndex - 
                                         (workingData.dataEndIndex - toIndex), 
                                       currentData.frontPadding, 
                                       currentData.rearPadding);
    
    return new ConcurrentArrayList<T>(newSet, 
                                      modificationLock);
  }
  
  @Override
  public String toString() {
    return currentData.toString();
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof ConcurrentArrayList) {
      ConcurrentArrayList cal = (ConcurrentArrayList)o;
      return currentData.equalsEquivelent(cal.currentData);
    } else if (o instanceof List) {
      List list = (List)o;
      if (list.size() != this.size()) {
        return false;
      }
      Iterator thisIt = this.iterator();
      Iterator listIt = list.iterator();
      while (thisIt.hasNext() && listIt.hasNext()) {
        if (! thisIt.next().equals(listIt.next())) {
          return false;
        }
      }
      if (thisIt.hasNext() || listIt.hasNext()) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    return currentData.hashCode();
  }
  
  /**
   * This is an iterator implementation that is designed to
   * iterate over a given dataSet.  Modifiable actions will attempt
   * to make changes to the parent class.
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class DataSetListIterator implements ListIterator<T> {
    private DataSet<T> dataSet;
    private int nextIndex;

    public DataSetListIterator(DataSet<T> dataSet, int index) {
      this.dataSet = dataSet;
      nextIndex = index;
    }

    @Override
    public boolean hasNext() {
      return nextIndex < dataSet.size;
    }

    @Override
    public T next() {
      verifyPosition();
      
      return dataSet.get(nextIndex++);
    }

    @Override
    public boolean hasPrevious() {
      return nextIndex - 1 >= 0;
    }

    @Override
    public T previous() {
      nextIndex--;
      
      verifyPosition();
      
      return dataSet.get(nextIndex);
    }
    
    private void verifyPosition() {
      if (nextIndex < 0 || nextIndex >= dataSet.size) {
        throw new NoSuchElementException();
      }
    }

    @Override
    public int nextIndex() {
      return nextIndex;
    }

    @Override
    public int previousIndex() {
      return nextIndex - 1;
    }

    @Override
    public void remove() {
      synchronized (modificationLock) {
        // you can not cause concurrent modification exceptions with this implementation
        if (currentData == dataSet) {
          ConcurrentArrayList.this.remove(--nextIndex);
          
          dataSet = currentData;
        } else {
          int globalIndex = ConcurrentArrayList.this.indexOf(dataSet.get(nextIndex - 1));
          if (globalIndex >= 0) {
            ConcurrentArrayList.this.remove(globalIndex);
          }
        }
      }
    }

    @Override
    public void set(T e) {
      synchronized (modificationLock) {
        if (currentData == dataSet) {
          ConcurrentArrayList.this.set(nextIndex - 1, e);
          
          dataSet = currentData;
        } else {
          int globalIndex = ConcurrentArrayList.this.indexOf(dataSet.get(nextIndex - 1));
          if (globalIndex >= 0) {
            ConcurrentArrayList.this.set(globalIndex, e);
          }
        }
      }
    }

    @Override
    public void add(T e) {
      synchronized (modificationLock) {
        if (currentData == dataSet) {
          ConcurrentArrayList.this.add(nextIndex, e);
          
          nextIndex++;
          
          dataSet = currentData;
        } else {
          int globalIndex = ConcurrentArrayList.this.indexOf(dataSet.get(nextIndex - 1));
          if (globalIndex >= 0) {
            ConcurrentArrayList.this.add(globalIndex + 1, e);
          }
        }
      }
    }
  }
  
  /**
   * This is designed to be an immutable version of the list.  
   * Modifiable actions will return a new instance that is based 
   * off this one.  Because the array may change in areas outside
   * of the scope of this dataArray, it is expected that the
   * modificationLock is held while any modifiable operations 
   * are happening.
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   * @param <T> type of object that is held
   */
  protected static class DataSet<T> {
    protected final Object[] dataArray;
    protected final int dataStartIndex; // inclusive
    protected final int dataEndIndex;   // exclusive
    protected final int size;
    private int frontPadding; // locked around modificationLock
    private int rearPadding; // locked around modificationLock
    
    protected DataSet(Object[] dataArray, 
                      int frontPadding, 
                      int rearPadding) {
      this(dataArray, frontPadding, 
           dataArray.length - rearPadding, 
           frontPadding, rearPadding);
    }

    protected DataSet(Object[] dataArray, 
                      int dataStartIndex, 
                      int dataEndIndex, 
                      int frontPadding, 
                      int rearPadding) {
      this.dataArray = dataArray;
      this.dataStartIndex = dataStartIndex;
      this.dataEndIndex = dataEndIndex;
      this.size = dataEndIndex - dataStartIndex;
      this.frontPadding = frontPadding;
      this.rearPadding = rearPadding;
    }
    
    /**
     * Returns a new {@link DataSet} that contains only relevant and active items in the data 
     * array.  The returned DataSet has the same set front and rear padding, so additional 
     * modifications will expand the array based off those set values.
     * 
     * @return New trimmed {@link DataSet}, or {@code this} if already trimmed
     */
    public DataSet<T> trimToSize() {
      if (dataStartIndex == 0 && dataEndIndex == dataArray.length) {
        return this;
      } else {
        Object[] newData = new Object[size];
        System.arraycopy(dataArray, dataStartIndex, newData, 0, size);
        return new DataSet<T>(newData, 0, size, frontPadding, rearPadding);
      }
    }

    /**
     * Call to reposition one index to a new index.
     * 
     * @param origCurrentIndex original index to move
     * @param origNewIndex new index position
     * @return a new {@link DataSet} which represents the change (or the same reference if the inputs are a no-op)
     */
    public DataSet<T> reposition(int origCurrentIndex, int origNewIndex) {
      if (size == 1) {
        // no-op, moving single item to same position
        return this;
      } else if (origNewIndex == size && origCurrentIndex == size - 1) {
        // no-op, moving end item to end
        return this;
      }
      
      int currentIndex = origCurrentIndex + dataStartIndex;
      int newIndex = origNewIndex + dataStartIndex;
      
      if (newIndex > currentIndex) {  // move right
        Object[] newData; // will not be allocated till necessary
        
        if (newIndex == dataEndIndex) {
          if (currentIndex == dataStartIndex && 
              dataArray.length - 1 > dataEndIndex && 
              dataArray[dataEndIndex] == null) {
            // reposition front item to end without an array copy
            dataArray[dataEndIndex] = dataArray[currentIndex];
            
            return new DataSet<T>(dataArray, dataStartIndex + 1, dataEndIndex + 1, 
                                  frontPadding, rearPadding);
          } else {
            newData = new Object[size + frontPadding + rearPadding];
            
            // moving to end can be done with two array copies at most
            System.arraycopy(dataArray, dataStartIndex, 
                             newData, frontPadding, origCurrentIndex);
            System.arraycopy(dataArray, currentIndex + 1, 
                             newData, frontPadding + origCurrentIndex, 
                             size - origCurrentIndex - 1);
          }
        } else {
          newData = new Object[size + frontPadding + rearPadding];
          
          // work backwards
          System.arraycopy(dataArray, newIndex,   // write from new position to end
                           newData, frontPadding + origNewIndex, 
                           size - origNewIndex);
          System.arraycopy(dataArray, currentIndex + 1, // write from removed position to new position
                           newData, frontPadding + origCurrentIndex, 
                           origNewIndex - origCurrentIndex);
          System.arraycopy(dataArray, dataStartIndex, // write from start to removed position
                           newData, frontPadding, 
                           origCurrentIndex);
        }
        
        newData[frontPadding + origNewIndex - 1] = dataArray[currentIndex];
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      } else if (newIndex < currentIndex) { // move left
        Object[] newData; // will not be allocated till necessary
        
        if (newIndex == dataStartIndex) {
          if (dataStartIndex > 0 && 
              currentIndex == dataEndIndex - 1 && 
              dataArray[dataStartIndex - 1] == null) {
            // reposition the end item to the front without an array copy
            dataArray[dataStartIndex - 1] = dataArray[currentIndex];
            
            return new DataSet<T>(dataArray, dataStartIndex - 1, dataEndIndex - 1, 
                                  frontPadding, rearPadding);
          } else {
            newData = new Object[size + frontPadding + rearPadding];
            
            // moving to front can be done with two array copies at most
            System.arraycopy(dataArray, dataStartIndex, 
                             newData, frontPadding + 1, origCurrentIndex);
            System.arraycopy(dataArray, currentIndex + 1, 
                             newData, frontPadding + origCurrentIndex + 1, 
                             dataEndIndex - currentIndex - 1);
          }
        } else {
          newData = new Object[size + frontPadding + rearPadding];
          
          // work forward
          System.arraycopy(dataArray, dataStartIndex,   // write from start to new position
                           newData, frontPadding, origNewIndex);
          System.arraycopy(dataArray, newIndex,   // write from new position to current position
                           newData, frontPadding + origNewIndex + 1, 
                           origCurrentIndex - origNewIndex);
          if (origCurrentIndex < size - 1) {
            System.arraycopy(dataArray, currentIndex + 1, // write from current position to end
                             newData, frontPadding + origCurrentIndex + 1, 
                             size - origCurrentIndex - 1);
          }
        }
        
        newData[frontPadding + origNewIndex] = dataArray[currentIndex];
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      } else {
        // no-op, moving to same position
        return this;
      }
    }
    
    /**
     * Used to get a newly sized array which copies the beginning items till 
     * the new size is reached (or all items if the newSize is larger).  The 
     * newly created array will also respect the front and rear padding, which 
     * is NOT included in the newSize provided.
     * 
     * @param newSize size for data within the new array
     * @return a new array
     */
    private Object[] getArrayCopy(int newSize) {
      Object[] newData = new Object[newSize + frontPadding + rearPadding];

      System.arraycopy(dataArray, dataStartIndex, 
                       newData, frontPadding, Math.min(size, newSize));
      
      return newData;
    }

    /**
     * Returns the item at a given index, this function 
     * handles the front padding for you.
     * 
     * @param index index of item
     * @return stored item
     */
    @SuppressWarnings("unchecked")
    public T get(int index) {
      return (T)dataArray[index + dataStartIndex];
    }

    /**
     * Call to check for the index of a given item.  The 
     * return index has already had the front padding removed 
     * from the actual index.
     * 
     * @param o Object to search for
     * @return index of item, or -1 if not found
     */
    public int indexOf(Object o) {
      for (int i = dataStartIndex; i < dataEndIndex; i++) {
        if (dataArray[i].equals(o)) {
          return i - dataStartIndex;
        }
      }
      
      return -1;
    }

    /**
     * Call to check for the last index of a given item.  The 
     * return index has already had the front padding removed 
     * from the actual index.
     * 
     * @param o Object to search for
     * @return index of item, or -1 if not found
     */
    public int lastIndexOf(Object o) {
      for (int i = dataEndIndex - 1; i >= dataStartIndex; i--) {
        if (dataArray[i].equals(o)) {
          return i - dataStartIndex;
        }
      }
      
      return -1;
    }

    /**
     * Sets a specific index with a given element.
     * 
     * @param index index to set the item at
     * @param element element to place in the array
     * @return a new {@link DataSet} which represents the change
     */
    public DataSet<T> set(int index, T element) {
      if (index == size) {
        return addToEnd(element);
      } else {
        Object[] newData = getArrayCopy(size);
        newData[index + frontPadding] = element;
      
        return new DataSet<T>(newData, frontPadding, rearPadding);
      }
    }

    /**
     * Adds an item to the front of the structure.
     * 
     * @param e item to be added
     * @return a new {@link DataSet} which represents the change
     */
    public DataSet<T> addToFront(T e) {
      if (dataStartIndex > 0 && dataArray[dataStartIndex - 1] == null) {
        // there is space in the current array
        dataArray[dataStartIndex - 1] = e;
        
        return new DataSet<T>(dataArray, dataStartIndex - 1, dataEndIndex, 
                              frontPadding, rearPadding);
      } else {
        Object[] newData = new Object[size + 1 + frontPadding + rearPadding];
        
        newData[frontPadding] = e;
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding + 1, 
                         size);
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      }
    }
    
    /**
     * Adds an item to the end of the structure.
     * 
     * @param e item to be added
     * @return a new {@link DataSet} which represents the change
     */
    public DataSet<T> addToEnd(T e) {
      if (dataArray.length - 1 >= dataEndIndex && dataArray[dataEndIndex] == null) {
        // there is space in the current array
        dataArray[dataEndIndex] = e;
        
        return new DataSet<T>(dataArray, dataStartIndex, dataEndIndex + 1, 
                              frontPadding, rearPadding);
      } else {
        Object[] newData = getArrayCopy(size + 1);
        
        newData[size + frontPadding] = e;
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      }
    }

    /**
     * Adds an item at a specific index within the structure.
     * 
     * @param origIndex index to place the item
     * @param element item to be added
     * @return a new {@link DataSet} which represents the change
     */
    public DataSet<T> add(int origIndex, T element) {
      if (origIndex == 0) { // add to front
        return addToFront(element);
      } else if (origIndex == size) { // add to end
        return addToEnd(element);
      } else {  // add into middle
        Object[] newData = new Object[size + 1 + frontPadding + rearPadding];
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding, origIndex);
        newData[frontPadding + origIndex] = element;
        System.arraycopy(dataArray, dataStartIndex + origIndex, 
                         newData, frontPadding + origIndex + 1, 
                         size - origIndex);
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      }
    }

    /**
     * Adds all items within the collection to the end of the structure.
     * 
     * @param c collection to add items from
     * @return a new {@link DataSet} which represents the change, or this reference if the collection is empty
     */
    public DataSet<T> addAll(Collection<? extends T> c) {
      return addAll(size, c);
    }

    /**
     * Adds all items within the collection at a given index.
     * 
     * @param origIndex index to start insertion at
     * @param c collection to add items from
     * @return a new {@link DataSet} which represents the change, or this reference if the collection is empty
     */
    public DataSet<T> addAll(int origIndex, Collection<? extends T> c) {
      if (c == null || c.isEmpty()) {
        return this;
      }
      
      Object[] toAdd = c.toArray();
      if (origIndex == 0) {
        // add to front
        if (toAdd.length <= dataStartIndex && 
            dataArray[dataStartIndex - 1] == null) {  // if previous one is null, all previous ones are null
          // we can copy the new items in, without copying our existing array
          System.arraycopy(toAdd, 0, 
                           dataArray, dataStartIndex - toAdd.length, 
                           toAdd.length);
          
          return new DataSet<T>(dataArray, 
                                dataStartIndex - toAdd.length, 
                                dataEndIndex, 
                                frontPadding, rearPadding);
        } else {
          Object[] newData = new Object[size + toAdd.length + 
                                          frontPadding + rearPadding];
          
          System.arraycopy(toAdd, 0, 
                           newData, frontPadding, toAdd.length);
          System.arraycopy(dataArray, dataStartIndex, 
                           newData, frontPadding + toAdd.length, size);
          
          return new DataSet<T>(newData, frontPadding, rearPadding);
        }
      } else if (origIndex == size) {
        // add to end
        if (dataEndIndex + toAdd.length <= dataArray.length && 
            dataArray[dataEndIndex] == null) {  // if next one is null, all future ones should be
          // we can copy the new items in, without copying our existing array
          System.arraycopy(toAdd, 0, 
                           dataArray, dataEndIndex, 
                           toAdd.length);

          return new DataSet<T>(dataArray, 
                                dataStartIndex, 
                                dataEndIndex + toAdd.length, 
                                frontPadding, rearPadding);
        } else {
          Object[] newData = getArrayCopy(size + toAdd.length);
          
          System.arraycopy(toAdd, 0, 
                           newData, size + frontPadding, toAdd.length);
          
          return new DataSet<T>(newData, frontPadding, rearPadding);
        }
      } else {
        // add in middle
        Object[] newData = new Object[size + toAdd.length + frontPadding + rearPadding];
        
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding, origIndex);
        System.arraycopy(toAdd, 0, 
                         newData, frontPadding + origIndex, toAdd.length);
        System.arraycopy(dataArray, dataStartIndex + origIndex, 
                         newData, frontPadding + origIndex + toAdd.length, 
                         size - origIndex);
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      }
    }
    
    /**
     * Removes a specific index from the collection.
     * 
     * @param origIndex index to remove from
     * @return a new {@link DataSet} which represents the change
     */
    public DataSet<T> remove(int origIndex) {
      int index = origIndex + dataStartIndex;
      
      if (index == dataStartIndex) {  // remove from front without copy
        return new DataSet<T>(dataArray, dataStartIndex + 1, dataEndIndex, 
                              frontPadding, rearPadding);
      } else if (index == dataEndIndex - 1) {  // remove from end without copy
        return new DataSet<T>(dataArray, dataStartIndex, dataEndIndex - 1, 
                              frontPadding, rearPadding);
      } else {  // remove from middle
        Object[] newData = new Object[size - 1 + frontPadding + rearPadding];
        
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding, origIndex);
        System.arraycopy(dataArray, index + 1, 
                         newData, frontPadding + origIndex, 
                         size - origIndex - 1);
        
        return new DataSet<T>(newData, frontPadding, rearPadding);
      }
    }

    /**
     * Removes a specific index from the collection.
     * 
     * @param c Collection which contains items to be removed
     * @return a new {@link DataSet} which represents the change, or the same reference if no modification was necessary
     */
    public DataSet<T> removeAll(Collection<?> c) {
      Object[] resultArray = null;  // will only be allocated once modification occurs
      
      int i = frontPadding;
      for (int currentIndex = 0; currentIndex < size; currentIndex++) {
        Object currItem = this.get(currentIndex);
        if (! c.contains(currItem)) {
          if (resultArray != null) {
            resultArray[i++] = currItem;
          } else {
            /* if result array has not been created yet, we will do a single array copy
             * once a modification occurs. 
             */
            i++;
          }
        } else {
          // modification occurred, create array and copy
          if (resultArray == null) {
            resultArray = new Object[size + frontPadding + rearPadding];
            System.arraycopy(dataArray, dataStartIndex, resultArray, frontPadding, i);
          }
        }
      }
      
      if (resultArray != null) {
        return new DataSet<T>(resultArray, frontPadding, i, 
                              frontPadding, rearPadding);
      } else {
        return this;
      }
    }

    /**
     * Keeps only the items in the provided collection.
     * 
     * @param c Collection to examine for items to retain
     * @return a new {@link DataSet} which represents the change, or the same reference if no modification was necessary
     */
    public DataSet<T> retainAll(Collection<?> c) {
      Object[] resultArray = null;  // will only be allocated once modification occurs
      
      int i = frontPadding;
      for (int currentIndex = 0; currentIndex < size; currentIndex++) {
        Object currItem = this.get(currentIndex);
        if (c.contains(currItem)) {
          if (resultArray != null) {
            resultArray[i++] = currItem;
          } else {
            i++;
          }
        } else {
          // modification occurred, create array and copy
          if (resultArray == null) {
            resultArray = new Object[size + frontPadding + rearPadding];
            System.arraycopy(dataArray, dataStartIndex, resultArray, frontPadding, i);
          }
        }
      }
      
      if (resultArray != null) {
        return new DataSet<T>(resultArray, frontPadding, i, 
                              frontPadding, rearPadding);
      } else {
        return this;
      }
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof DataSet) {
        @SuppressWarnings("rawtypes")
        DataSet ds = (DataSet)o;
        return equalsEquivelent(ds);
      } else {
        return false;
      }
    }
    
    /**
     * Call to see if the lists are equivalently equal, meaning that 
     * they have the same items in the same order.  This is what most 
     * equals operations for lists expect.
     * 
     * @param ds Other DataSet to compare against (can't be null)
     * @return {@code true} if they are equal
     */
    @SuppressWarnings("rawtypes")
    public boolean equalsEquivelent(DataSet ds) {
      if (this.size != ds.size) {
        return false;
      }
      
      for (int i = 0; i < size; i++) {
        Object thisItem = this.get(i);
        Object thatItem = ds.get(i);
        if ((thisItem == null && thatItem != null) || 
            (thisItem != null && ! thisItem.equals(thatItem))) {
          return false;
        }
      }
      return true;
    }
    
    /**
     * This is a call to check if the DataSet is exactly equal.  This is 
     * unique for the design of this collection.  This can be a quicker 
     * check, but may result in saying DataSet's are not equal, when they 
     * basically are equal.
     * 
     * @param ds Other DataSet to compare against (can't be null)
     * @return {@code true} if they are equal
     */
    @SuppressWarnings("rawtypes")
    public boolean equalsExactly(DataSet ds) {
      if (this == ds) {
        return true;
      } else {
        if (dataStartIndex != ds.dataStartIndex || 
            dataEndIndex != ds.dataEndIndex || 
            dataArray.length != ds.dataArray.length) {
          return false;
        } else {
          for (int i = 0; i < dataArray.length; i++) {
            if ((dataArray[i] == null && ds.dataArray[i] != null) || 
                (dataArray[i] != null && ! dataArray[i].equals(ds.dataArray[i]))) {
              return false;
            }
          }
          return true;
        }
      }
    }
    
    @Override
    public int hashCode() {
      int hashCode = 1;
      for (int i = dataStartIndex; i < dataEndIndex; i++) {
        Object obj = dataArray[i];
        hashCode = HASH_CODE_PRIME_NUMBER * hashCode + (obj == null ? 0 : obj.hashCode());
      }
      
      return hashCode;
    }
    
    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      
      result.append('[');
      for (int i = 0; i < dataArray.length; i++) {
        if (i != 0) {
          result.append(", ");
        }
        if (i == dataStartIndex) {
          result.append('S');
        }
        result.append(i).append('-').append(dataArray[i]);
        if (i == dataEndIndex - 1) {
          result.append('E');
        }
      }
      result.append(']');
      
      return result.toString();
    }
  }
}

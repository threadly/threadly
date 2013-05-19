package org.threadly.concurrent;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
/**
 * A thread safe list implementation with an array back end.  Make sure
 * to read the java docs carefully, as many functions behave subtly different
 * from the java.util.List definition.
 * 
 * The design of this implementation is NOT to completely avoid synchronization.  
 * We have a hybrid implementation of volatile and synchronized to allow for cheaper
 * reading, but keeping high consistency.  It works with the idea that the internal
 * data is immutable.  Each read has an immutable version of the data.  Thus making 
 * writes more expensive (almost like a CopyOnWriteArrayList).
 * 
 * The difference between this and a CopyOnWriteArrayList is that it does allow some 
 * synchronization.  Which can give higher consistency guarantees for some operations.
 * 
 * A couple notable points is that subList calls are very cheap, but modifications
 * to sublist are completely independent from their source list.
 * 
 * Unlike CopyOnWriteArrayList, Iterators can attempt to modify the state of the backing
 * structure (assuming it still makes sense to do so).  Although unlike CopyOnWriteArrayList 
 * iterators, once an Iterator is created it will never see updates to the structure.  
 * For that reason it is impossible to have a ConcurrentModificationExcception.
 * 
 * @author jent - Mike Jensen
 *
 * @param <T> type of object to retain
 */
public class ConcurrentArrayList<T> implements List<T>, Deque<T>, RandomAccess {
  protected static <E> DataSet<E> makeEmptyDataSet(int frontPadding, int rearPadding) {
    if (frontPadding < 0) {
      throw new IllegalArgumentException("frontPadding must be >= 0");
    } else if (rearPadding < 0) {
      throw new IllegalArgumentException("rearPadding must be >= 0");
    }
    
    return new DataSet<E>(new Object[0], 0, 0, frontPadding, rearPadding);
  }
  
  protected final VirtualLock modificationLock;
  protected volatile DataSet<T> currentData;
  
  /**
   * Constructs a new ConcurrentArrayList with a new
   * internal NativeLock implementation.
   */
  public ConcurrentArrayList() {
    this(0, 0);
  }
  
  /**
   * Constructs a new ConcurrentArrayList with a new
   * internal NativeLock implementation.  Specifying
   * the padding amounts can optimize this implementation 
   * more for the specific use case.  If there is space in the 
   * array for adds to the front or end, then we are 
   * able to avoid an array copy.
   * 
   * @param frontPadding padding to add to front of array to possible avoid array copies
   * @param rearPadding padding to add to end of array to possible avoid array copies
   */
  public ConcurrentArrayList(int frontPadding, int rearPadding) {
    this(new NativeLock(), frontPadding, rearPadding);
  }

  /**
   * Constructs a new ConcurrentArrayList with a provided
   * lock implementation.
   * 
   * @param modificationLock lock to synchronize on internally
   */
  public ConcurrentArrayList(VirtualLock modificationLock) {
    this(modificationLock, 0, 0);
  }

  /**
   * Constructs a new ConcurrentArrayList with a provided
   * lock implementation.  Specifying the padding amounts 
   * can optimize this implementation more for the 
   * specific use case.  If there is space in the array 
   * for adds to the front or end, then we are able to 
   * avoid an array copy.
   * 
   * @param modificationLock lock to synchronize on internally
   * @param frontPadding padding to add to front of array to possible avoid array copies
   * @param rearPadding padding to add to end of array to possible avoid array copies
   */
  public ConcurrentArrayList(VirtualLock modificationLock, 
                             int frontPadding, int rearPadding) {
    this(ConcurrentArrayList.<T>makeEmptyDataSet(frontPadding, 
                                                 rearPadding), 
         modificationLock);
  }
  
  protected ConcurrentArrayList(DataSet<T> startSet, 
                                VirtualLock modificationLock) {
    if (startSet == null) {
      throw new IllegalArgumentException("Must provide starting dataSet");
    } else if (modificationLock == null) {
      modificationLock = new NativeLock();
    }
    
    this.modificationLock = modificationLock;
    currentData = startSet;
  }
  
  /**
   * If you want to chain multiple calls together and
   * ensure that no threads modify the structure during 
   * that time you can get the lock to prevent additional 
   * modifications.
   * 
   * @return lock used internally
   */
  public VirtualLock getModificationLock() {
    return modificationLock;
  }
  
  /**
   * This changes the configuration for the front padding amount for 
   * future modification operations.
   * 
   * @param frontPadding New value to over allocate the front of new buffers
   */
  public void setFrontPadding(int frontPadding) {
    if (frontPadding < 0) {
      throw new IllegalArgumentException("frontPadding must be >= 0");
    }
    
    synchronized (modificationLock) {
      currentData.frontPadding = frontPadding;
    }
  }

  /**
   * This changes the configuration for the rear padding amount for 
   * future modification operations.
   * 
   * @param rearPadding New value to over allocate the rear of new buffers
   */
  public void setRearPadding(int rearPadding) {
    if (rearPadding < 0) {
      throw new IllegalArgumentException("rearPadding must be >= 0");
    }

    synchronized (modificationLock) {
      currentData.rearPadding = rearPadding;
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
    return currentData.get(index);
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
    Object[] toCopyArray = currentData.dataArray;
    Object[] resultArray = new Object[toCopyArray.length];
    System.arraycopy(toCopyArray, 0, resultArray, 0, resultArray.length);
    
    return resultArray;
  }

  @Override
  public <E> E[] toArray(E[] a) {
    Object[] toCopyArray = currentData.dataArray;
    if (a.length < toCopyArray.length) {  // TODO - need to implement this
      throw new UnsupportedOperationException("need " + toCopyArray.length + ", provided " + a.length);
    }
    
    System.arraycopy(toCopyArray, 0, a, 0, toCopyArray.length);
    
    return a;
  }

  @Override
  public boolean add(T e) {
    if (e == null) {
      return false;
    }
    
    synchronized (modificationLock) {
      currentData = currentData.add(e);
    }
    
    return true;
  }

  @Override
  public boolean remove(Object o) {
    return removeFirstOccurrence(o);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }
    
    Iterator<? extends T> it = c.iterator();
    while (it.hasNext()) {
      if (it.next() == null) {
        it.remove();
      }
    }

    synchronized (modificationLock) {
      currentData = currentData.addAll(c);
    }
    
    return true;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }
    
    Iterator<? extends T> it = c.iterator();
    while (it.hasNext()) {
      if (it.next() == null) {
        it.remove();
      }
    }

    synchronized (modificationLock) {
      if (index > size()) {
        throw new IndexOutOfBoundsException("Index is beyond the array size: " + index);
      } else if (index < 0) {
        throw new IndexOutOfBoundsException("Index can not be negative");
      }
      
      currentData = currentData.addAll(index, c);
    }
    
    return true;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }

    synchronized (modificationLock) {
      DataSet<T> originalSet = currentData;
      currentData = currentData.removeAll(c);
      
      return currentData != originalSet;
    }
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }

    synchronized (modificationLock) {
      DataSet<T> originalSet = currentData;
      currentData = currentData.retainAll(c);
      
      return currentData != originalSet;
    }
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
    add(0, e);
  }

  @Override
  public void addLast(T e) {
    add(e);
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
        currentData = currentData.remove(size() - 1);
      }
      
      return result;
    }
  }

  @Override
  public T pollLast() {
    synchronized (modificationLock) {
      T result = peekLast();
      if (result != null) {
        currentData = currentData.remove(size() - 1);
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
  public boolean removeFirstOccurrence(Object o) {
    if (o == null) {
      return false;
    }
    
    synchronized (modificationLock) {
      int index = currentData.indexOf(o);
      if (index < 0) {
        return false;
      } else {
        currentData = currentData.remove(index);
        return true;
      }
    }
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    if (o == null) {
      return false;
    }
    
    synchronized (modificationLock) {
      int index = currentData.lastIndexOf(o);
      if (index < 0) {
        return false;
      } else {
        currentData = currentData.remove(index);
        return true;
      }
    }
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
  public Iterator<T> descendingIterator() {
    final ListIterator<T> li = listIterator(size());
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

  @Override
  public T set(int index, T element) {
    DataSet<T> originalSet;
    synchronized (modificationLock) {
      if (index > size() - 1) {
        throw new IndexOutOfBoundsException("Index is beyond the array max index: " + index);
      } else if (index < 0) {
        throw new IndexOutOfBoundsException("Index can not be negative");
      }
      
      originalSet = currentData;
      currentData = currentData.set(index, element);
    }
    
    return originalSet.get(index);
  }

  @Override
  public void add(int index, T element) {
    synchronized (modificationLock) {
      if (index > size()) {
        throw new IndexOutOfBoundsException("Index is beyond the array size: " + index);
      } else if (index < 0) {
        throw new IndexOutOfBoundsException("Index can not be negative");
      }
      
      currentData = currentData.add(index, element);
    }
  }

  @Override
  public T remove(int index) {
    DataSet<T> originalSet;
    synchronized (modificationLock) {
      if (index > size() - 1) {
        throw new IndexOutOfBoundsException("Index is beyond the array max index: " + index);
      } else if (index < 0) {
        throw new IndexOutOfBoundsException("Index can not be negative");
      }
      
      originalSet = currentData;
      currentData = currentData.remove(index);
    }
    
    return originalSet.get(index);
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
    synchronized (modificationLock) {
      if (newIndex > size()) {
        throw new IndexOutOfBoundsException(newIndex + " is beyond the array's length: " + (size() - 1));
      }
      
      int index;
      if (searchBackwards) {
        index = lastIndexOf(item);
      } else {
        index = indexOf(item);
      }
      
      if (index < 0) {
        throw new RuntimeException("Could not find item: " + item);
      }
      
      reposition(index, newIndex);
    }
  }
  
  /**
   * Move a stored item located at an index to a new index.  
   * Provide the size for newIndex to move the item to the end of 
   * the list.  Otherwise all items after the new index will 
   * be shifted right.
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
    
    synchronized (modificationLock) {
      if (newIndex > size()) {
        throw new IndexOutOfBoundsException("new index " + newIndex + 
                                              " is beyond the array's length: " + (size() - 1));
      } else if (originalIndex > size()) {
        throw new IndexOutOfBoundsException("original index " + originalIndex + 
                                              " is beyond the array's length: " + (size() - 1));
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

  /**
   * This returns a sub list from the current list.  The initial call
   * is very cheap because it uses the current data backing to produce 
   * (no copying necessary).  But any modifications to this list will 
   * be treated as a completely new list, and wont ever reflect on the 
   * source list.
   */
  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    DataSet<T> workingData = currentData;
    DataSet<T> newSet = new DataSet<T>(workingData.dataArray, 
                                       workingData.dataStartIndex + fromIndex, 
                                       workingData.dataEndIndex - 
                                         (workingData.dataEndIndex - toIndex), 
                                       currentData.frontPadding, 
                                       currentData.rearPadding);
    // TODO - do we want to return an unmodifiable list?
    return new ConcurrentArrayList<T>(newSet, 
                                      modificationLock);
  }
  
  @Override
  public String toString() {
    return currentData.toString();
  }
  
  /**
   * This is an iterator implementation that is designed to
   * iterate over a given dataSet.  Modifiable actions will attempt
   * to make changes to the parent class.
   * 
   * @author jent - Mike Jensen
   */
  protected class DataSetListIterator implements ListIterator<T> {
    private final DataSet<T> dataSet;
    private int currentIndex;

    public DataSetListIterator(DataSet<T> dataSet, int index) {
      this.dataSet = dataSet;
      currentIndex = index;
    }

    @Override
    public boolean hasNext() {
      return currentIndex + 1 < dataSet.size;
    }

    @Override
    public T next() {
      currentIndex++;
      
      return dataSet.get(currentIndex);
    }

    @Override
    public boolean hasPrevious() {
      return currentIndex - 1 >= 0;
    }

    @Override
    public T previous() {
      currentIndex--;
      
      return dataSet.get(currentIndex);
    }

    @Override
    public int nextIndex() {
      return currentIndex + 1;
    }

    @Override
    public int previousIndex() {
      return currentIndex - 1;
    }

    @Override
    public void remove() {
      // you can not cause concurrent modification exceptions with this implementation
      ConcurrentArrayList.this.remove(dataSet.get(currentIndex));
    }

    @Override
    public void set(T e) {
      synchronized (modificationLock) {
        int globalIndex = ConcurrentArrayList.this.indexOf(dataSet.get(currentIndex));
        if (globalIndex >= 0) {
          ConcurrentArrayList.this.set(globalIndex, e);
        }
      }
    }

    @Override
    public void add(T e) {
      synchronized (modificationLock) {
        int globalIndex = ConcurrentArrayList.this.indexOf(dataSet.get(currentIndex));
        if (globalIndex >= 0) {
          ConcurrentArrayList.this.add(globalIndex + 1, e);
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
   * 
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

    public DataSet<T> reposition(int origCurrentIndex, int origNewIndex) {
      int currentIndex = origCurrentIndex + dataStartIndex;
      int newIndex = origNewIndex + dataStartIndex;
      
      if (newIndex > currentIndex) {  // move right
        Object[] newData = new Object[size + frontPadding + rearPadding];
        
        if (newIndex == dataEndIndex) {
          System.arraycopy(dataArray, dataStartIndex, 
                           newData, frontPadding, origCurrentIndex);
          System.arraycopy(dataArray, currentIndex + 1, 
                           newData, frontPadding + origCurrentIndex, 
                           size - origCurrentIndex - 1);
        } else {
          //work backwards
          // shift end for placement of new item
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
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      } else if (newIndex < currentIndex) { // move left
        Object[] newData = new Object[size + frontPadding + rearPadding];
        
        if (newIndex == dataStartIndex) {
          System.arraycopy(dataArray, dataStartIndex, 
                           newData, frontPadding + 1, origCurrentIndex);
          System.arraycopy(dataArray, currentIndex + 1, 
                           newData, frontPadding + origCurrentIndex + 1, 
                           dataEndIndex - currentIndex - 1);
        } else {
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
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      } else {  // equal
        return this;
      }
    }
    
    private Object[] getArrayCopy(int newSize) {
      Object[] newData = new Object[newSize + frontPadding + rearPadding];

      System.arraycopy(dataArray, dataStartIndex, 
                       newData, frontPadding, Math.min(size, newSize));
      
      return newData;
    }

    @SuppressWarnings("unchecked")
    public T get(int index) {
      index += dataStartIndex;
      
      return (T)dataArray[index];
    }

    public int indexOf(Object o) {
      for (int i = dataStartIndex; i < dataEndIndex; i++) {
        if (dataArray[i].equals(o)) {
          return i - dataStartIndex;
        }
      }
      
      return -1;
    }

    public int lastIndexOf(Object o) {
      for (int i = dataEndIndex - 1; i >= dataStartIndex; i--) {
        if (dataArray[i].equals(o)) {
          return i - dataStartIndex;
        }
      }
      
      return -1;
    }

    public DataSet<T> set(int index, T element) {
      if (index == size) {
        return add(element);
      } else {
        Object[] newData = getArrayCopy(size);
        newData[index + frontPadding] = element;
      
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      }
    }

    public DataSet<T> add(T e) {
      int index = size;
      if (dataArray.length - 1 < index || dataArray[index] != null) {
        Object[] newData = getArrayCopy(index + 1);
        
        newData[index + frontPadding] = e;
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      } else {
        // there is space in the current array
        dataArray[index] = e;
        
        return new DataSet<T>(dataArray, dataStartIndex, dataEndIndex + 1, frontPadding, rearPadding);
      }
    }

    public DataSet<T> add(int origIndex, T element) {
      Object[] newData;
      if (origIndex == 0) {
        // add to front
        newData = new Object[size + 1 + frontPadding + rearPadding];
        newData[frontPadding] = element;
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding + 1, 
                         size);
      } else if (origIndex == size) {
        // add to end
        return add(element);
      } else {
        newData = new Object[size + 1 + frontPadding + rearPadding];
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding, origIndex);
        newData[frontPadding + origIndex] = element;
        System.arraycopy(dataArray, dataStartIndex + origIndex, 
                         newData, frontPadding + origIndex + 1, 
                         size - origIndex);
      }
      
      return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
    }

    public DataSet<T> addAll(Collection<? extends T> c) {
      return addAll(size, c);
    }

    public DataSet<T> addAll(int origIndex, Collection<? extends T> c) {
      Object[] toAdd = c.toArray();
      if (origIndex == 0) {
        // add to front
        Object[] newData = new Object[size + toAdd.length + frontPadding + rearPadding];
        
        System.arraycopy(toAdd, 0, 
                         newData, frontPadding, toAdd.length);
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding + toAdd.length, size);
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      } else if (origIndex == size) {
        Object[] newData = getArrayCopy(size + toAdd.length);
        
        System.arraycopy(toAdd, 0, 
                         newData, size + frontPadding, toAdd.length);
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      } else {
        Object[] newData = new Object[size + toAdd.length + frontPadding + rearPadding];
        
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding, origIndex);
        System.arraycopy(toAdd, 0, 
                         newData, frontPadding + origIndex, toAdd.length);
        System.arraycopy(dataArray, dataStartIndex + origIndex, 
                         newData, frontPadding + origIndex + toAdd.length, 
                         size - origIndex);
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      }
    }
    
    public DataSet<T> remove(int origIndex) {
      int index = origIndex + dataStartIndex;
      
      if (index == dataStartIndex) {
        return new DataSet<T>(dataArray, dataStartIndex + 1, dataEndIndex, frontPadding, rearPadding);
      } else if (index == dataEndIndex - 1) {
        return new DataSet<T>(dataArray, dataStartIndex, dataEndIndex - 1, frontPadding, rearPadding);
      } else {
        Object[] newData = new Object[size - 1 + frontPadding + rearPadding];
        
        System.arraycopy(dataArray, dataStartIndex, 
                         newData, frontPadding, origIndex);
        System.arraycopy(dataArray, index + 1, 
                         newData, frontPadding + origIndex, 
                         size - origIndex - 1);
        
        return new DataSet<T>(newData, frontPadding, newData.length - rearPadding, frontPadding, rearPadding);
      }
    }

    // TODO - this can be optimized
    public DataSet<T> removeAll(Collection<?> c) {
      DataSet<T> result = this;
      
      Iterator<?> it = c.iterator();
      while (it.hasNext()) {
        Object o = it.next();
        int index = result.indexOf(o);
        while (index >= 0) {
          result = result.remove(index);
          
          index = result.indexOf(o);
        }
      }
      
      return result;
    }

    public DataSet<T> retainAll(Collection<?> c) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
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
      return toString().hashCode();
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
        if (dataArray[i] instanceof Delayed) {
          result.append(i).append('-')
                .append(((Delayed)dataArray[i]).getDelay(TimeUnit.MILLISECONDS))
                .append(';').append(dataArray[i]);
        } else {
          result.append(i).append('-').append(dataArray[i]);
        }
        if (i == dataEndIndex - 1) {
          result.append('E');
        }
      }
      result.append(']');
      
      return result.toString();
    }
  }
}

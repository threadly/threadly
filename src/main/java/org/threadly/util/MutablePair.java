package org.threadly.util;

/**
 * A special type of {@link Pair} which allows the stored references to be updated after 
 * creation.  Note that stored references are not {@code volatile} or {@code synchronized}, so 
 * thread access must be guarded in synchronization.
 * 
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
public class MutablePair<L, R> extends Pair<L, R> {
  /**
   * Constructs a new mutable pair with the left and right references defaulted to be {@code null}.
   */
  public MutablePair() {
    super(null, null);
  }

  /**
   * Constructs a new mutable pair, providing the left and right objects to be held.
   * 
   * @param left Left reference
   * @param right Right reference
   */
  public MutablePair(L left, R right) {
    super(left, right);
  }
  
  /**
   * Update the left reference with the provided object.
   * 
   * @param left New reference to be used for the left of the pair
   */
  public void setLeft(L left) {
    this.left = left;
  }

  /**
   * Update the right reference with the provided object.
   * 
   * @param right New reference to be used for the right of the pair
   */
  public void setRight(R right) {
    this.right = right;
  }
}

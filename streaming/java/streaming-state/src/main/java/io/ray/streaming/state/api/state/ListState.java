package io.ray.streaming.state.api.state;

import java.util.List;

/** Description <br> */
public interface ListState<T> extends State {

  /**
   * Appends the specified element to the end of this list.
   *
   * @param ele element to be appended to this list
   * @throws Exception
   */
  void add(T ele) throws Exception;

  /**
   * Appends all of the elements in the specified collection to the end of this list, in the order
   * that they are returned by the specified collection's iterator.
   *
   * @param list list containing elements to be added to this list
   * @throws Exception
   */
  void addAll(List<T> list) throws Exception;

  /**
   * Returns the element at the specified position in this list.
   *
   * @param index the index of the element to be return
   */
  T get(int index) throws Exception;

  /**
   * Removes the element at the specified position in this list
   *
   * @param index the index of the element to be removed
   */
  T remove(int index) throws Exception;

  /**
   * Returns <tt>true</tt> if this listState contains no elements.
   *
   * @return <tt>true</tt> if this listState contains no elements.
   */
  boolean isEmpty() throws Exception;

  /**
   * Returns the number of elements in this list.
   *
   * @return the number of elements in this list
   */
  int size() throws Exception;

  /** Removes all of the elements from this list */
  void clear() throws Exception;

  /**
   * Appends the specified element to the index of this list.
   *
   * @param index the index of the element to be set
   * @param ele element to be appended to this list
   * @throws Exception
   */
  void set(int index, T ele) throws Exception;
}

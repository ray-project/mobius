package io.ray.streaming.state.typeinfo.serializer;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import java.io.IOException;
import java.io.Serializable;

/**
 * Type serializer abstract base class.
 */
public abstract class TypeSerializer<T> implements Serializable {

  /**
   * Serialize the given given record.
   */
  public abstract void serialize(T record, DataOutputView outputView) throws IOException;

  /**
   * Deserialize a record from the given byte array.
   */
  public abstract T deserialize(DataInputView inputView) throws IOException;

  /**
   * Creates a deep copy of this serializer.
   *
   * Because this serializer might be used in several threads. In order to ensure thread safety, we
   * need to use this method to use an independent serializer in each thread.
   */
  public abstract TypeSerializer<T> duplicate();


  /**
   * Create a new instance of the data type class.
   */
  public abstract T createInstance();
}

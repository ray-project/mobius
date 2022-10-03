package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/** Type serializer for string. */
public class StringSerializer extends TypeSerializer<String> {

  private static final int HIGH_BIT = 0x1 << 7;

  public static final StringSerializer INSTANCE = new StringSerializer();

  private static final String DEFAULT_VALUE = "";

  @Override
  public void serialize(String record, DataOutputView outputView) throws IOException {
    writeString(record, outputView);
  }

  @Override
  public String deserialize(DataInputView inputView) throws IOException {
    return readString(inputView);
  }

  private void writeString(String str, DataOutputView outputView) throws IOException {
    if (str != null) {
      int strLen = str.length();

      // because zero indicates str is null, so add 1 to the length to indicate the length of str.
      int lenToWrite = strLen + 1;
      if (lenToWrite < 0) {
        throw new IllegalArgumentException("String is too long");
      }

      // The maximum write of byte is 127
      while (lenToWrite >= HIGH_BIT) {
        // Write 7 bits each time, the 8th bit is forced to 1
        outputView.write(lenToWrite | HIGH_BIT);
        lenToWrite >>>= 7;
      }
      // Write the part less than 128.
      outputView.write(lenToWrite);

      for (int i = 0; i < str.length(); i++) {
        int c = str.charAt(i);
        while (c >= HIGH_BIT) {
          outputView.write(c | HIGH_BIT);
          c >>>= 7;
        }
        outputView.write(c);
      }
    } else {
      outputView.write(0);
    }
  }

  public String readString(DataInputView inputView) throws IOException {
    int len = inputView.readUnsignedByte();

    // Zero means the str is null
    if (len == 0) {
      return null;
    }

    if (len >= HIGH_BIT) {
      int shift = 7;
      int current;
      len = len & 0x7f;
      while ((current = inputView.readUnsignedByte()) >= HIGH_BIT) {
        len |= (current & 0x7f) << shift;
        shift += 7;
      }
      len |= current << shift;
    }

    // because write len add 1
    len -= 1;

    final char[] data = new char[len];
    for (int i = 0; i < len; i++) {
      int c = inputView.readUnsignedByte();
      if (c >= HIGH_BIT) {
        int shift = 7;
        int current;
        c = c & 0x7f;
        while ((current = inputView.readUnsignedByte()) >= HIGH_BIT) {
          c |= (current & 0x7f) << shift;
          shift += 7;
        }
        c |= current << shift;
        data[i] = (char) c;
      } else {
        data[i] = (char) c;
      }
    }
    return new String(data, 0, len);
  }

  @Override
  public TypeSerializer<String> duplicate() {
    return null;
  }

  @Override
  public String createInstance() {
    return DEFAULT_VALUE;
  }
}

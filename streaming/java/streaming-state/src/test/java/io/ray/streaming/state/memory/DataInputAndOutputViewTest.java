package io.ray.streaming.state.memory;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataInputAndOutputViewTest {

  private static byte[] testByteArray;

  String testStrBytes = "hello";
  String testChars = "world";
  String testUtf = "hello world";
  byte[] testBytes = {0, 1, 2, 3};

  @Test
  public void testDataOutputView() throws IOException {
    DataOutputView outputView = new DataOutputView();
    long length = 0;
    length += 1;
    outputView.writeByte(2);
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeChar('c');
    length += 2;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeShort(255);
    length += 2;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeInt(300);
    length += 4;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeLong(999999L);
    length += 8;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeFloat(3.14f);
    length += 4;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeDouble(3.1415926);
    length += 8;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeBoolean(false);
    length += 1;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeBytes(testStrBytes);
    length += testStrBytes.length();
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeChars(testChars);
    length += testChars.length() * 2;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.writeUTF(testUtf);
    // because write str length
    length += testUtf.length() + 2;
    Assert.assertEquals(outputView.getPosition(), length);

    outputView.write(testBytes);
    length += testBytes.length;
    Assert.assertEquals(outputView.getPosition(), length);

    testByteArray = outputView.getCopyOfBuffer();
  }

  @Test(dependsOnMethods = "testDataOutputView")
  public void testDataInputView() throws IOException {
    DataInputView inputView = new DataInputView();
    inputView.setBuffer(testByteArray);

    byte readByte = inputView.readByte();
    Assert.assertEquals(readByte, 2);

    char readChar = inputView.readChar();
    Assert.assertEquals(readChar, 'c');

    short readShort = inputView.readShort();
    Assert.assertEquals(readShort, 255);

    int readInt = inputView.readInt();
    Assert.assertEquals(readInt, 300);

    long readLong = inputView.readLong();
    Assert.assertEquals(readLong, 999999L);

    float readFloat = inputView.readFloat();
    Assert.assertEquals(readFloat, 3.14f);

    double readDouble = inputView.readDouble();
    Assert.assertEquals(readDouble, 3.1415926);

    boolean readBoolean = inputView.readBoolean();
    Assert.assertFalse(readBoolean);

    inputView.skipBytes(testStrBytes.length());
    inputView.skipBytes(testChars.length() * 2);

    String readUtf = inputView.readUTF();
    Assert.assertEquals(readUtf, testUtf);

    byte[] readByteArray = new byte[testBytes.length];
    inputView.readFully(readByteArray);
    for (int i = 0; i < testBytes.length; i++) {
      Assert.assertEquals(readByteArray[i], testBytes[i]);
    }
  }
}

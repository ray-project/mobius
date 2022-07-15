package io.ray.streaming.state.typeinfo.serializer.old;

import org.apache.hadoop.hbase.util.Bytes;

public class DefaultKeyMapSerializer<K, UK, UV> extends AbstractSerDe {
  
  public DefaultKeyMapSerializer() {
    setSerializerType("Kryo");
  }

  public byte[] serKey(K key) {
    return  Bytes.toBytes(key.toString());
  }

  public byte[] serUKey(UK uk) {
    return  Bytes.toBytes(uk.toString());
  }

  public UK deSerUKey(byte[] ukArray) {
    return (UK) Bytes.toString(ukArray);
  }

  public byte[] serUValue(UV uv) {
    return (byte[]) uv;
  }

  public UV deSerUValue(byte[] uvArray) {
    return (UV)uvArray;
  }
}

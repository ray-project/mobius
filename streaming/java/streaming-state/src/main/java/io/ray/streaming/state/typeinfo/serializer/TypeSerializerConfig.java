package io.ray.streaming.state.typeinfo.serializer;

import com.google.common.base.MoreObjects;
import io.ray.state.typeinfo.serializer.kryo.KryoRegistration;
import java.util.List;

/**
 * Configuration information related to type information.
 */
public class TypeSerializerConfig {

  private List<KryoRegistration> kryoRegistrations;

  public TypeSerializerConfig(List<KryoRegistration> kryoRegistrations) {
    this.kryoRegistrations = kryoRegistrations;
  }

  public List<KryoRegistration> getKryoRegistrations() {
    return kryoRegistrations;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("kryoRegistrations", kryoRegistrations)
        .toString();
  }
}

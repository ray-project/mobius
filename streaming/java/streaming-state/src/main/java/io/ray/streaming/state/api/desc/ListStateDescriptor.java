package io.ray.streaming.state.api.desc;

import io.ray.streaming.state.api.StateType;
import io.ray.streaming.state.typeinfo.MapTypeInfo;
import java.util.Map;

/** Description <br> */
public class ListStateDescriptor<V> extends AbstractStateDescriptor<Map<String, V>> {

  public static <V> ListStateDescriptor<V> build(String stateName, Class<V> valueClass) {

    return new ListStateDescriptor<>(stateName, new MapTypeInfo<>(String.class, valueClass));
  }

  private ListStateDescriptor(String stateName, MapTypeInfo<String, V> typeInfo) {
    super(stateName, typeInfo);
  }

  @Override
  public StateType getStateType() {
    return StateType.VALUE;
  }
}

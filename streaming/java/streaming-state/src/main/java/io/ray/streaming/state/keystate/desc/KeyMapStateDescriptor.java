package io.ray.streaming.state.keystate.desc;

import io.ray.streaming.state.keystate.state.KeyMapState;
import java.util.Map;

public class KeyMapStateDescriptor<K, UK, UV>
    extends AbstractStateDescriptor<KeyMapState<K, UK, UV>,  Map<K, Map<UK, UV>>> {

  private KeyMapStateDescriptor(
      String stateName,
      Class<K> keyClass,
      Class<UK> subKeyClass,
      Class<UV> valueClass
  ) {
    // TODO: use the types to help serde
    super(stateName, null);
  }

  public static <K, UK, UV> KeyMapStateDescriptor<K, UK, UV> build(String stateName,
                                                                   Class<K> keyClass,
                                                                   Class<UK> subKeyClass,
                                                                   Class<UV> valueClass) {
    return new KeyMapStateDescriptor<>(stateName,
        keyClass, subKeyClass, valueClass);
  }

  @Override
  public StateType getStateType() {
    return StateType.KEY_MAP;
  }

}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ray.streaming.state.api.desc;

import io.ray.streaming.state.api.StateType;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MapStateDescriptor. */
public class MapStateDescriptor<K, V> extends AbstractStateDescriptor<Map<K, V>> {

  private static final Logger LOG = LoggerFactory.getLogger(MapStateDescriptor.class);

  public MapStateDescriptor(String name, Class<K> keyType, Class<V> valueType) {
    super(name, null);
    if (LOG.isInfoEnabled()) {
      LOG.info("KeyType " + keyType + "valueType" + valueType);
    }
    // TODO: use the types to help serde
  }

  public static <K, V> MapStateDescriptor<K, V> build(
      String name, Class<K> keyType, Class<V> valueType) {
    return new MapStateDescriptor<>(name, keyType, valueType);
  }

  @Override
  public StateType getStateType() {
    return StateType.MAP;
  }
}

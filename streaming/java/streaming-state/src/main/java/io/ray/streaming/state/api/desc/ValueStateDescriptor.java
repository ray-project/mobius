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
import io.ray.streaming.state.typeinfo.MapTypeInfo;
import java.util.Map;

/** ValueStateDescriptor. */
public class ValueStateDescriptor<V> extends AbstractStateDescriptor<Map<Object, V>> {

  private ValueStateDescriptor(String stateName, MapTypeInfo<Object, V> typeInfo) {
    super(stateName, typeInfo);
  }

  public static <V> ValueStateDescriptor<V> build(String stateName, Class<V> valueClass) {
    return new ValueStateDescriptor<>(stateName, new MapTypeInfo<>(Object.class, valueClass));
  }

  @Override
  public StateType getStateType() {
    return StateType.VALUE;
  }
}

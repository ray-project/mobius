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

import com.google.common.base.MoreObjects;
import io.ray.streaming.state.api.StateType;
import io.ray.streaming.state.typeinfo.TypeInformation;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State describes the abstract base class, provides access to state name, type information, etc.
 */
public abstract class AbstractStateDescriptor<T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStateDescriptor.class);

  /**
   * The current state name.
   */
  private final String stateName;

  /**
   * The current state type info.
   */
  private TypeInformation<T> typeInfo;

  public AbstractStateDescriptor(String stateName, TypeInformation<T> typeInfo) {
    this.stateName = stateName;
    this.typeInfo = typeInfo;
  }

  public String getStateName() {
    return stateName;
  }

  public TypeInformation<T> getTypeInfo() {
    return typeInfo;
  }

  public abstract StateType getStateType();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("stateName", stateName)
        .add("typeInfo", typeInfo).toString();
  }
}
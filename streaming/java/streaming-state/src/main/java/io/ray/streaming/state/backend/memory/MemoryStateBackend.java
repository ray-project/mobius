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

package io.ray.streaming.state.backend.memory;


import io.ray.streaming.state.backend.StateBackend;
import io.ray.streaming.state.serialization.KeyMapStoreSerializer;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memory storage backend, implementing snapshot,restore，etc.
 */
public class MemoryStateBackend implements StateBackend {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStateBackend.class);


  @Override
  public void init() {
    LOG.info("Finish init buffer state backend.");
  }

  @Override
  public CompletableFuture<Boolean> snapshot(long snapshotId) {
    return CompletableFuture.supplyAsync(() -> {
      LOG.info("Memory store backend not support checkpoint.");
      return false;
    });
  }

  @Override
  public void rollbackSnapshot(long snapshotId) {
    LOG.info("Memory store backend not support restore.");
  }

  @Override
  public boolean deleteSnapshot(long snapshotId) {
    return true;
  }

  @Override
  public void close() {
  }
}
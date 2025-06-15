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

package org.apache.paimon.lookup.memory;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.ListState;
import org.apache.paimon.lookup.SetState;
import org.apache.paimon.lookup.StateFactory;
import org.apache.paimon.lookup.ValueState;

import java.io.IOException;

/** Factory to create in-memory state. */
public class InMemoryStateFactory implements StateFactory {

    @Override
    public <K, V> ValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        return new InMemoryValueState<>(keySerializer, valueSerializer);
    }

    @Override
    public <K, V> SetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        return new InMemorySetState<>(keySerializer, valueSerializer);
    }

    @Override
    public <K, V> ListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        return new InMemoryListState<>(keySerializer, valueSerializer);
    }

    @Override
    public boolean preferBulkLoad() {
        return false;
    }

    @Override
    public void close() throws IOException {}
}

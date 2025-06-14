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

package org.apache.paimon.lookup;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.memory.InMemoryStateFactory;
import org.apache.paimon.lookup.rocksdb.RocksDBStateFactory;
import org.apache.paimon.options.Options;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.paimon.CoreOptions.LOOKUP_CACHE_IN_MEMORY;

/** State factory to create {@link State}. */
public interface StateFactory extends Closeable {

    <K, V> ValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException;

    <K, V> SetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException;

    <K, V> ListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException;

    boolean preferBulkLoad();

    static StateFactory create(String diskDir, Options options) throws IOException {
        if (options.get(LOOKUP_CACHE_IN_MEMORY)) {
            return new InMemoryStateFactory();
        } else {
            return new RocksDBStateFactory(diskDir, options, null);
        }
    }
}

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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexPathFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Cache for index {@link PathFactory}s. */
public class IndexFilePathFactories {

    private final Map<Pair<BinaryRow, Integer>, IndexPathFactory> cache = new ConcurrentHashMap<>();
    private final FileStorePathFactory pathFactory;

    public IndexFilePathFactories(FileStorePathFactory pathFactory) {
        this.pathFactory = pathFactory;
    }

    public IndexPathFactory get(BinaryRow partition, int bucket) {
        return cache.computeIfAbsent(
                Pair.of(partition, bucket),
                k -> pathFactory.indexFileFactory(k.getKey(), k.getValue()));
    }
}

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
import org.apache.paimon.io.DataFilePathFactory;

import java.util.HashMap;
import java.util.Map;

/** Cache for {@link DataFilePathFactory}s. */
public class DataFilePathFactories {

    private final Map<Pair<BinaryRow, Integer>, DataFilePathFactory> cache = new HashMap<>();
    private final FileStorePathFactory pathFactory;

    public DataFilePathFactories(FileStorePathFactory pathFactory) {
        this.pathFactory = pathFactory;
    }

    public DataFilePathFactory get(BinaryRow partition, int bucket) {
        return cache.computeIfAbsent(
                Pair.of(partition, bucket),
                k -> pathFactory.createDataFilePathFactory(k.getKey(), k.getValue()));
    }
}

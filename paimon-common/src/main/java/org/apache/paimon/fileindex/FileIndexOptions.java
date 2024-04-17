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

package org.apache.paimon.fileindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Options of file index column. */
public class FileIndexOptions {

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final long fileIndexInManifestThreshold;

    private final Map<String, Map<String, Options>> indexTypeOptions;

    public FileIndexOptions() {
        this(CoreOptions.FILE_INDEX_IN_MANIFEST_THRESHOLD.defaultValue().getBytes());
    }

    public FileIndexOptions(long fileIndexInManifestThreshold) {
        this.indexTypeOptions = new HashMap<>();
        this.fileIndexInManifestThreshold = fileIndexInManifestThreshold;
    }

    public void computeIfAbsent(String column, String indexType) {
        indexTypeOptions
                .computeIfAbsent(column, c -> new HashMap<>())
                .computeIfAbsent(indexType, i -> new Options());
    }

    public Options get(String column, String indexType) {
        return Optional.ofNullable(indexTypeOptions.getOrDefault(column, null))
                .map(x -> x.get(indexType))
                .orElse(null);
    }

    public boolean isEmpty() {
        return indexTypeOptions.isEmpty();
    }

    public long fileIndexInManifestThreshold() {
        return fileIndexInManifestThreshold;
    }

    public Set<Map.Entry<String, Map<String, Options>>> entrySet() {
        return indexTypeOptions.entrySet().stream()
                .filter(entry -> !entry.getKey().contains(FileIndexCommon.JUNCTION_SYMBOL))
                .collect(Collectors.toSet());
    }
}

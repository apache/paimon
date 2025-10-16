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
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.table.source.DeletionFile;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Cache for deletion vector meta. */
public class DVMetaCache {
    private final Cache<MetaCacheKey, List<DVMetaCacheValue>> cache;

    public DVMetaCache(long maxElementSize) {
        this.cache =
                Caffeine.newBuilder().maximumSize(maxElementSize).executor(Runnable::run).build();
    }

    @Nullable
    public Map<String, DeletionFile> read(Path path, BinaryRow partition, int bucket) {
        MetaCacheKey metaCacheKey = new MetaCacheKey(path, partition, bucket);
        List<DVMetaCacheValue> cacheValue = this.cache.getIfPresent(metaCacheKey);
        if (cacheValue == null) {
            return null;
        }
        // If the bucket doesn't have dv metas, return empty set.
        Map<String, DeletionFile> dvFilesMap = new HashMap<>();
        cacheValue.forEach(
                dvMeta ->
                        dvFilesMap.put(
                                dvMeta.getFileName(),
                                new DeletionFile(
                                        dvMeta.dataFileName(),
                                        dvMeta.offset(),
                                        dvMeta.length(),
                                        dvMeta.cardinality())));
        return dvFilesMap;
    }

    public void put(
            Path path, BinaryRow partition, int bucket, Map<String, DeletionFile> dvFilesMap) {
        MetaCacheKey key = new MetaCacheKey(path, partition, bucket);
        List<DVMetaCacheValue> cacheValue = new ArrayList<>();
        dvFilesMap.forEach(
                (fileName, file) -> {
                    DVMetaCacheValue dvMetaCacheValue =
                            new DVMetaCacheValue(
                                    fileName,
                                    file.path(),
                                    (int) file.offset(),
                                    (int) file.length(),
                                    file.cardinality());
                    cacheValue.add(dvMetaCacheValue);
                });
        this.cache.put(key, cacheValue);
    }

    private static class DVMetaCacheValue extends DeletionVectorMeta {
        private final String fileName;

        public DVMetaCacheValue(
                String fileName,
                String dataFileName,
                int start,
                int length,
                @Nullable Long cardinality) {
            super(dataFileName, start, length, cardinality);
            this.fileName = fileName;
        }

        public String getFileName() {
            return fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DVMetaCacheValue)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            DVMetaCacheValue that = (DVMetaCacheValue) o;
            return Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fileName);
        }
    }
}

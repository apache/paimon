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
    private final Cache<DVMetaCacheKey, List<DVMetaCacheValue>> cache;

    public DVMetaCache(long maxElementSize) {
        this.cache =
                Caffeine.newBuilder()
                        .maximumSize(maxElementSize)
                        .softValues()
                        .executor(Runnable::run)
                        .build();
    }

    @Nullable
    public Map<String, DeletionFile> read(Path path, BinaryRow partition, int bucket) {
        DVMetaCacheKey cacheKey = new DVMetaCacheKey(path, partition, bucket);
        List<DVMetaCacheValue> cacheValue = this.cache.getIfPresent(cacheKey);
        if (cacheValue == null) {
            return null;
        }
        // If the bucket doesn't have dv metas, return empty set.
        Map<String, DeletionFile> dvFilesMap = new HashMap<>();
        cacheValue.forEach(
                dvMeta ->
                        dvFilesMap.put(
                                dvMeta.getDataFileName(),
                                new DeletionFile(
                                        dvMeta.getDeletionFilePath(),
                                        dvMeta.getOffset(),
                                        dvMeta.getLength(),
                                        dvMeta.getCardinality())));
        return dvFilesMap;
    }

    public void put(
            Path path, BinaryRow partition, int bucket, Map<String, DeletionFile> dvFilesMap) {
        DVMetaCacheKey key = new DVMetaCacheKey(path, partition, bucket);
        List<DVMetaCacheValue> cacheValue = new ArrayList<>();
        dvFilesMap.forEach(
                (dataFileName, deletionFile) -> {
                    DVMetaCacheValue dvMetaCacheValue =
                            new DVMetaCacheValue(
                                    dataFileName,
                                    deletionFile.path(),
                                    (int) deletionFile.offset(),
                                    (int) deletionFile.length(),
                                    deletionFile.cardinality());
                    cacheValue.add(dvMetaCacheValue);
                });
        this.cache.put(key, cacheValue);
    }

    private static class DVMetaCacheValue {
        private final String dataFileName;
        private final String deletionFilePath;
        private final int offset;
        private final int length;
        @Nullable private final Long cardinality;

        public DVMetaCacheValue(
                String dataFileName,
                String deletionFilePath,
                int start,
                int length,
                @Nullable Long cardinality) {
            this.dataFileName = dataFileName;
            this.deletionFilePath = deletionFilePath;
            this.offset = start;
            this.length = length;
            this.cardinality = cardinality;
        }

        public String getDataFileName() {
            return dataFileName;
        }

        public String getDeletionFilePath() {
            return deletionFilePath;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }

        @Nullable
        public Long getCardinality() {
            return cardinality;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DVMetaCacheValue that = (DVMetaCacheValue) o;
            return offset == that.offset
                    && length == that.length
                    && Objects.equals(dataFileName, that.dataFileName)
                    && Objects.equals(deletionFilePath, that.deletionFilePath)
                    && Objects.equals(cardinality, that.cardinality);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataFileName, deletionFilePath, offset, length, cardinality);
        }
    }

    /** Cache key for deletion vector meta at bucket level. */
    private static final class DVMetaCacheKey {
        private final Path path;
        private final BinaryRow row;
        private final int bucket;

        public DVMetaCacheKey(Path path, BinaryRow row, int bucket) {
            this.path = path;
            this.row = row;
            this.bucket = bucket;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DVMetaCacheKey)) {
                return false;
            }
            DVMetaCacheKey that = (DVMetaCacheKey) o;
            return bucket == that.bucket
                    && Objects.equals(path, that.path)
                    && Objects.equals(row, that.row);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, row, bucket);
        }
    }
}

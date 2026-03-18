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

import java.util.Map;
import java.util.Objects;

/** Cache for deletion vector meta. */
public class DVMetaCache {

    private final Cache<DVMetaCacheKey, Map<String, DeletionFile>> cache;

    public DVMetaCache(long maxValueNumber) {
        this.cache =
                Caffeine.newBuilder()
                        .weigher(DVMetaCache::weigh)
                        .maximumWeight(maxValueNumber)
                        .softValues()
                        .executor(Runnable::run)
                        .build();
    }

    private static int weigh(DVMetaCacheKey cacheKey, Map<String, DeletionFile> cacheValue) {
        return cacheValue.size() + 1;
    }

    @Nullable
    public Map<String, DeletionFile> read(Path manifestPath, BinaryRow partition, int bucket) {
        DVMetaCacheKey cacheKey = new DVMetaCacheKey(manifestPath, partition, bucket);
        return this.cache.getIfPresent(cacheKey);
    }

    public void put(
            Path path, BinaryRow partition, int bucket, Map<String, DeletionFile> dvFilesMap) {
        DVMetaCacheKey key = new DVMetaCacheKey(path, partition, bucket);
        this.cache.put(key, dvFilesMap);
    }

    /** Cache key for deletion vector meta at bucket level. */
    private static final class DVMetaCacheKey {

        private final Path manifestPath;
        private final BinaryRow row;
        private final int bucket;

        public DVMetaCacheKey(Path manifestPath, BinaryRow row, int bucket) {
            this.manifestPath = manifestPath;
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
                    && Objects.equals(manifestPath, that.manifestPath)
                    && Objects.equals(row, that.row);
        }

        @Override
        public int hashCode() {
            return Objects.hash(manifestPath, row, bucket);
        }
    }
}

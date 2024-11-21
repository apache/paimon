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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.tag.Tag;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/** Cache for {@link Snapshot} and {@link Tag} and {@link TableSchema}. */
public class MetaCacheManager {

    public static final Cache<Path, Snapshot> SNAPSHOT_CACHE =
            Caffeine.newBuilder()
                    .softValues()
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .maximumSize(300)
                    .executor(Runnable::run)
                    .build();

    public static final Cache<Path, Tag> TAG_CACHE =
            Caffeine.newBuilder()
                    .softValues()
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .maximumSize(100)
                    .executor(Runnable::run)
                    .build();

    public static final Cache<Path, TableSchema> SCHEMA_CACHE =
            Caffeine.newBuilder()
                    .softValues()
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .maximumSize(100)
                    .executor(Runnable::run)
                    .build();

    public static void invalidateCacheForPrefix(Path tablePath) {
        String path = tablePath.toString();
        invalidateCacheForPrefix(SNAPSHOT_CACHE, path);
        invalidateCacheForPrefix(TAG_CACHE, path);
        invalidateCacheForPrefix(SCHEMA_CACHE, path);
    }

    private static void invalidateCacheForPrefix(Cache<Path, ?> cache, String tablePath) {
        List<Path> keys =
                cache.asMap().keySet().stream()
                        .filter(key -> key.toString().startsWith(tablePath))
                        .collect(Collectors.toList());
        cache.invalidateAll(keys);
    }
}

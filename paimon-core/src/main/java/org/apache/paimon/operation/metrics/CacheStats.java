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

package org.apache.paimon.operation.metrics;

public class CacheStats {
    private boolean hitDatabaseCache;
    private boolean hitTableCache;
    private boolean hitPartitionCache;

    private long databaseCacheSize;
    private long tableCacheSize;
    private long manifestCacheSize;
    private long partitionCacheSize;

    public CacheStats() {}

    // The following 3 stats may have concurrent problems, so engine side should lock when calling get methods.
    public boolean hitDatabaseCache() {
        return hitDatabaseCache;
    }

    public void setHitDatabaseCache(boolean hitDatabaseCache) {
        this.hitDatabaseCache = hitDatabaseCache;
    }

    public boolean hitTableCache() {
        return hitTableCache;
    }

    public void setHitTableCache(boolean hitTableCache) {
        this.hitTableCache = hitTableCache;
    }

    public boolean hitPartitionCache() {
        return hitPartitionCache;
    }

    public void setHitPartitionCache(boolean hitPartitionCache) {
        this.hitPartitionCache = hitPartitionCache;
    }

    public long getDatabaseCacheSize() {
        return databaseCacheSize;
    }

    public void setDatabaseCacheSize(long databaseCacheSize) {
        this.databaseCacheSize = databaseCacheSize;
    }

    public long getTableCacheSize() {
        return tableCacheSize;
    }

    public void setTableCacheSize(long tableCacheSize) {
        this.tableCacheSize = tableCacheSize;
    }

    public long getManifestCacheSize() {
        return manifestCacheSize;
    }

    public void setManifestCacheSize(long manifestCacheSize) {
        this.manifestCacheSize = manifestCacheSize;
    }

    public long getPartitionCacheSize() {
        return partitionCacheSize;
    }

    public void setPartitionCacheSize(long partitionCacheSize) {
        this.partitionCacheSize = partitionCacheSize;
    }
}
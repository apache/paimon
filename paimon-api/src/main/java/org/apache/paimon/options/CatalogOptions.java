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

package org.apache.paimon.options;

import org.apache.paimon.table.CatalogTableType;

import java.time.Duration;

import static org.apache.paimon.options.ConfigOptions.key;

/** Options for catalog. */
public class CatalogOptions {

    public static final ConfigOption<String> WAREHOUSE =
            ConfigOptions.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The warehouse root path of catalog.");

    public static final ConfigOption<String> METASTORE =
            ConfigOptions.key("metastore")
                    .stringType()
                    .defaultValue("filesystem")
                    .withDescription(
                            "Metastore of paimon catalog, supports filesystem, hive and jdbc.");

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Uri of metastore server.");

    public static final ConfigOption<CatalogTableType> TABLE_TYPE =
            ConfigOptions.key("table.type")
                    .enumType(CatalogTableType.class)
                    .defaultValue(CatalogTableType.MANAGED)
                    .withDescription("Type of table.");

    public static final ConfigOption<Boolean> LOCK_ENABLED =
            ConfigOptions.key("lock.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Enable Catalog Lock.");

    public static final ConfigOption<String> LOCK_TYPE =
            ConfigOptions.key("lock.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Lock Type for Catalog, such as 'hive', 'zookeeper'.");

    public static final ConfigOption<Duration> LOCK_CHECK_MAX_SLEEP =
            key("lock-check-max-sleep")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(8))
                    .withDescription("The maximum sleep time when retrying to check the lock.");

    public static final ConfigOption<Duration> LOCK_ACQUIRE_TIMEOUT =
            key("lock-acquire-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(8))
                    .withDescription("The maximum time to wait for acquiring the lock.");

    public static final ConfigOption<Integer> CLIENT_POOL_SIZE =
            key("client-pool-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Configure the size of the connection pool.");

    public static final ConfigOption<Boolean> CACHE_ENABLED =
            key("cache-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Controls whether the catalog will cache databases, tables, manifests and partitions.");

    public static final ConfigOption<Duration> CACHE_EXPIRE_AFTER_ACCESS =
            key("cache.expire-after-access")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withFallbackKeys("cache.expiration-interval")
                    .withDescription(
                            "Cache expiration policy: marks cache entries to expire after a specified duration has passed since their last access.");

    public static final ConfigOption<Duration> CACHE_EXPIRE_AFTER_WRITE =
            key("cache.expire-after-write")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "Cache expiration policy: marks cache entries to expire after a specified duration has passed since their last refresh.");

    public static final ConfigOption<Long> CACHE_PARTITION_MAX_NUM =
            key("cache.partition.max-num")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Controls the max number for which partitions in the catalog are cached.");

    public static final ConfigOption<MemorySize> CACHE_MANIFEST_SMALL_FILE_MEMORY =
            key("cache.manifest.small-file-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Controls the cache memory to cache small manifest files.");

    public static final ConfigOption<MemorySize> CACHE_MANIFEST_SMALL_FILE_THRESHOLD =
            key("cache.manifest.small-file-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription("Controls the threshold of small manifest file.");

    public static final ConfigOption<MemorySize> CACHE_MANIFEST_MAX_MEMORY =
            key("cache.manifest.max-memory")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("Controls the maximum memory to cache manifest content.");

    public static final ConfigOption<Integer> CACHE_SNAPSHOT_MAX_NUM_PER_TABLE =
            key("cache.snapshot.max-num-per-table")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "Controls the max number for snapshots per table in the catalog are cached.");

    public static final ConfigOption<Integer> CACHE_DV_MAX_NUM =
            key("cache.deletion-vectors.max-num")
                    .intType()
                    .defaultValue(100_000)
                    .withDescription(
                            "Controls the maximum number of deletion vector meta that can be cached.");

    public static final ConfigOption<Boolean> CASE_SENSITIVE =
            ConfigOptions.key("case-sensitive")
                    .booleanType()
                    .noDefaultValue()
                    .withFallbackKeys("allow-upper-case")
                    .withDescription("Indicates whether this catalog is case-sensitive.");

    public static final ConfigOption<Boolean> SYNC_ALL_PROPERTIES =
            ConfigOptions.key("sync-all-properties")
                    .booleanType()
                    // We should set default value to true in case of hive metastore losing table
                    // properties.
                    .defaultValue(true)
                    .withDescription("Sync all table properties to hive metastore");

    public static final ConfigOption<Boolean> FORMAT_TABLE_ENABLED =
            ConfigOptions.key("format-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to support format tables, format table corresponds to a regular csv, parquet or orc table, allowing read and write operations. "
                                    + "However, during these processes, it does not connect to the metastore; hence, newly added partitions will not be reflected in"
                                    + " the metastore and need to be manually added as separate partition operations.");

    public static final ConfigOption<Boolean> RESOLVING_FILE_IO_ENABLED =
            ConfigOptions.key("resolving-file-io.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable resolving fileio, when this option is enabled, in conjunction with the table's property data-file.external-paths, "
                                    + "Paimon can read and write to external storage paths, such as OSS or S3. "
                                    + "In order to access these external paths correctly, you also need to configure the corresponding access key and secret key.");

    public static final ConfigOption<Boolean> FILE_IO_ALLOW_CACHE =
            ConfigOptions.key("file-io.allow-cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to allow static cache in file io implementation. If not allowed, this means that "
                                    + "there may be a large number of FileIO instances generated, enabling caching can "
                                    + "lead to resource leakage.");
}

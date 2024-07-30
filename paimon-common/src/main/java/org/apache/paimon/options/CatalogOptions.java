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

import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.TextElement;
import org.apache.paimon.table.TableType;

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

    public static final ConfigOption<TableType> TABLE_TYPE =
            ConfigOptions.key("table.type")
                    .enumType(TableType.class)
                    .defaultValue(TableType.MANAGED)
                    .withDescription("Type of table.");

    public static final ConfigOption<Boolean> LOCK_ENABLED =
            ConfigOptions.key("lock.enabled")
                    .booleanType()
                    .defaultValue(false)
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

    public static final ConfigOption<Boolean> FS_ALLOW_HADOOP_FALLBACK =
            key("fs.allow-hadoop-fallback")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Allow to fallback to hadoop File IO when no file io found for the scheme.");

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
                            "Controls whether the catalog will cache table entries upon load.");

    public static final ConfigOption<Duration> CACHE_EXPIRATION_INTERVAL_MS =
            key("cache.expiration-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "Controls the duration for which entries in the catalog are cached.");

    public static final ConfigOption<String> LINEAGE_META =
            key("lineage-meta")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The lineage meta to store table and data lineage information.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Possible values:")
                                    .linebreak()
                                    .list(
                                            TextElement.text(
                                                    "\"jdbc\": Use standard jdbc to store table and data lineage information."))
                                    .list(
                                            TextElement.text(
                                                    "\"custom\": You can implement LineageMetaFactory and LineageMeta to store lineage information in customized storage."))
                                    .build());

    public static final ConfigOption<Boolean> ALLOW_UPPER_CASE =
            ConfigOptions.key("allow-upper-case")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Indicates whether this catalog allow upper case, "
                                    + "its default value depends on the implementation of the specific catalog.");
}

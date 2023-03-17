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

package org.apache.flink.table.store.options;

import org.apache.flink.table.store.table.TableType;

import java.time.Duration;

import static org.apache.flink.table.store.options.ConfigOptions.key;

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
                            "Metastore of table store catalog, supports filesystem and hive.");

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
}

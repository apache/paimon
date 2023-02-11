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

import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.FileIOLoader;
import org.apache.flink.table.store.fs.hadoop.HadoopFileIOLoader;
import org.apache.flink.table.store.table.TableType;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.store.options.ConfigOptions.key;

/** Configuration for {@link FileIO}. */
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

    private final Options options;
    private final Configuration hadoopConf;
    @Nullable private final FileIOLoader fallbackIOLoader;

    public CatalogOptions(
            Options options, Configuration hadoopConf, @Nullable FileIOLoader fallbackIOLoader) {
        this.options = options;
        this.hadoopConf = hadoopConf;
        if (fallbackIOLoader == null && get(FS_ALLOW_HADOOP_FALLBACK)) {
            this.fallbackIOLoader = new HadoopFileIOLoader();
        } else {
            this.fallbackIOLoader = fallbackIOLoader;
        }
    }

    public static CatalogOptions empty() {
        return create(new Options());
    }

    public static CatalogOptions create(Options options) {
        return create(options, new Configuration());
    }

    public static CatalogOptions create(Options options, Configuration hadoopConf) {
        return create(options, hadoopConf, null);
    }

    public static CatalogOptions create(
            Options options, Configuration hadoopConf, @Nullable FileIOLoader fallbackIOLoader) {
        return new CatalogOptions(options, hadoopConf, fallbackIOLoader);
    }

    public Options options() {
        return options;
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key.
     */
    @Nullable
    public String get(String key) {
        return options.get(key);
    }

    public <T> T get(ConfigOption<T> option) {
        return options.get(option);
    }

    public String get(String key, String defaultValue) {
        String value = get(key);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    /** Returns a {@link Set} view of the keys contained in this map. */
    public Set<String> keySet() {
        return options.keySet();
    }

    /** Return hadoop {@link Configuration}. */
    public Configuration hadoopConf() {
        return hadoopConf;
    }

    @Nullable
    public FileIOLoader fallbackIO() {
        return fallbackIOLoader;
    }

    public Map<String, String> toMap() {
        return options.toMap();
    }
}

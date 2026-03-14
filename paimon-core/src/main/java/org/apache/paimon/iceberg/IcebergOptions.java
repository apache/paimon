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

package org.apache.paimon.iceberg;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;
import org.apache.paimon.utils.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.ConfigOptions.key;

/** Config options for Paimon Iceberg compatibility. */
public class IcebergOptions {

    public static final String REST_CONFIG_PREFIX = "metadata.iceberg.rest.";

    public static final ConfigOption<StorageType> METADATA_ICEBERG_STORAGE =
            key("metadata.iceberg.storage")
                    .enumType(StorageType.class)
                    .defaultValue(StorageType.DISABLED)
                    .withDescription(
                            "When set, produce Iceberg metadata after a snapshot is committed, "
                                    + "so that Iceberg readers can read Paimon's raw data files.");

    public static final ConfigOption<StorageLocation> METADATA_ICEBERG_STORAGE_LOCATION =
            key("metadata.iceberg.storage-location")
                    .enumType(StorageLocation.class)
                    .noDefaultValue()
                    .withDescription(
                            "To store Iceberg metadata in a separate directory or under table location");

    public static final ConfigOption<Integer> FORMAT_VERSION =
            ConfigOptions.key("metadata.iceberg.format-version")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The format version of iceberg table, the value can be 2 or 3. "
                                    + "Note that only version 3 supports deletion vector.");

    public static final ConfigOption<Integer> COMPACT_MIN_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.min.file-num")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Minimum number of Iceberg manifest metadata files to trigger manifest metadata compaction.");

    public static final ConfigOption<Integer> COMPACT_MAX_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.max.file-num")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "If number of small Iceberg manifest metadata files exceeds this limit, "
                                    + "always trigger manifest metadata compaction regardless of their total size.");

    public static final ConfigOption<Boolean> METADATA_DELETE_AFTER_COMMIT =
            key("metadata.iceberg.delete-after-commit.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to delete old metadata files after each table commit");

    public static final ConfigOption<Integer> METADATA_PREVIOUS_VERSIONS_MAX =
            key("metadata.iceberg.previous-versions-max")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The number of old metadata files to keep after each table commit. "
                                    + "For rest-catalog, it will keep 1 old metadata at least.");

    public static final ConfigOption<String> URI =
            key("metadata.iceberg.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive metastore uri for Iceberg Hive catalog.");

    public static final ConfigOption<String> HIVE_CONF_DIR =
            key("metadata.iceberg.hive-conf-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hive-conf-dir for Iceberg Hive catalog.");

    public static final ConfigOption<String> HADOOP_CONF_DIR =
            key("metadata.iceberg.hadoop-conf-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hadoop-conf-dir for Iceberg Hive catalog.");

    public static final ConfigOption<String> MANIFEST_COMPRESSION =
            key("metadata.iceberg.manifest-compression")
                    .stringType()
                    .defaultValue(
                            "snappy") // some Iceberg reader cannot support zstd, for example DuckDB
                    .withDescription("Compression for Iceberg manifest files.");

    public static final ConfigOption<Boolean> MANIFEST_LEGACY_VERSION =
            key("metadata.iceberg.manifest-legacy-version")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Should use the legacy manifest version to generate Iceberg's 1.4 manifest files.");

    public static final ConfigOption<String> HIVE_CLIENT_CLASS =
            key("metadata.iceberg.hive-client-class")
                    .stringType()
                    .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
                    .withDescription("Hive client class name for Iceberg Hive Catalog.");

    public static final ConfigOption<String> METASTORE_DATABASE =
            key("metadata.iceberg.database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Metastore database name for Iceberg Catalog. "
                                    + "Set this as an iceberg database alias if using a centralized Catalog.");

    public static final ConfigOption<String> METASTORE_TABLE =
            key("metadata.iceberg.table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Metastore table name for Iceberg Catalog."
                                    + "Set this as an iceberg table alias if using a centralized Catalog.");

    public static final ConfigOption<Boolean> GLUE_SKIP_ARCHIVE =
            key("metadata.iceberg.glue.skip-archive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip archive for AWS Glue catalog.");

    public static final ConfigOption<Boolean> HIVE_SKIP_UPDATE_STATS =
            key("metadata.iceberg.hive-skip-update-stats")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip updating Hive stats.");

    private final Options options;

    public IcebergOptions(Map<String, String> options) {
        this(Options.fromMap(options));
    }

    public IcebergOptions(Options options) {
        this.options = options;
    }

    public Map<String, String> icebergRestConfig() {
        Map<String, String> restConfig = new HashMap<>();
        options.keySet()
                .forEach(
                        key -> {
                            if (key.startsWith(REST_CONFIG_PREFIX)) {
                                String restConfigKey = key.substring(REST_CONFIG_PREFIX.length());
                                Preconditions.checkArgument(
                                        !restConfigKey.isEmpty(),
                                        "config key '%s' for iceberg rest catalog is empty!",
                                        key);
                                restConfig.put(restConfigKey, options.get(key));
                            }
                        });
        return restConfig;
    }

    public boolean deleteAfterCommitEnabled() {
        return options.get(METADATA_DELETE_AFTER_COMMIT);
    }

    public int previousVersionsMax() {
        return options.get(METADATA_PREVIOUS_VERSIONS_MAX);
    }

    /** Where to store Iceberg metadata. */
    public enum StorageType implements DescribedEnum {
        DISABLED("disabled", "Disable Iceberg compatibility support."),
        TABLE_LOCATION("table-location", "Store Iceberg metadata in each table's directory."),
        HADOOP_CATALOG(
                "hadoop-catalog",
                "Store Iceberg metadata in a separate directory. "
                        + "This directory can be specified as the warehouse directory of an Iceberg Hadoop catalog."),
        HIVE_CATALOG(
                "hive-catalog",
                "Not only store Iceberg metadata like hadoop-catalog, "
                        + "but also create Iceberg external table in Hive."),
        REST_CATALOG(
                "rest-catalog",
                "Store Iceberg metadata in a REST catalog. "
                        + "This allows integration with Iceberg REST catalog services.");

        private final String value;
        private final String description;

        StorageType(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return TextElement.text(description);
        }
    }

    /** Where to store Iceberg metadata. */
    public enum StorageLocation implements DescribedEnum {
        TABLE_LOCATION(
                "table-location",
                "Store Iceberg metadata in each table's directory. Useful for standalone "
                        + "Iceberg tables or Java API access. Can also be used with Hive Catalog"),
        CATALOG_STORAGE(
                "catalog-location",
                "Store Iceberg metadata in a separate directory. "
                        + "Allows integration with Hive Catalog or Hadoop Catalog.");

        private final String value;
        private final String description;

        StorageLocation(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return TextElement.text(description);
        }
    }

    /**
     * Returns all ConfigOption fields defined in this class. This method uses reflection to
     * dynamically discover all ConfigOption fields, ensuring that new options are automatically
     * included without code changes.
     */
    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = IcebergOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(IcebergOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }
}

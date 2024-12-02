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
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;

import static org.apache.paimon.options.ConfigOptions.key;

/** Config options for Paimon Iceberg compatibility. */
public class IcebergOptions {

    public static final ConfigOption<StorageType> METADATA_ICEBERG_STORAGE =
            key("metadata.iceberg.storage")
                    .enumType(StorageType.class)
                    .defaultValue(StorageType.DISABLED)
                    .withDescription(
                            "When set, produce Iceberg metadata after a snapshot is committed, "
                                    + "so that Iceberg readers can read Paimon's raw data files.");

    public static final ConfigOption<Integer> COMPACT_MIN_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.min.file-num")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Minimum number of Iceberg metadata files to trigger metadata compaction.");

    public static final ConfigOption<Integer> COMPACT_MAX_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.max.file-num")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "If number of small Iceberg metadata files exceeds this limit, "
                                    + "always trigger metadata compaction regardless of their total size.");

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
                            "gzip") // some Iceberg reader cannot support zstd, for example DuckDB
                    .withDescription("Compression for Iceberg manifest files.");

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
                        + "but also create Iceberg external table in Hive.");

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
}

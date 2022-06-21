/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.store.utils.OptionsUtils.formatEnumOption;

/** Options for {@link FileStore}. */
public class FileStoreOptions implements Serializable {

    public static final String TABLE_STORE_PREFIX = "table-store.";

    public static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Bucket number for file store.");

    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of this table in the filesystem.");

    public static final ConfigOption<String> FILE_FORMAT =
            ConfigOptions.key("file.format")
                    .stringType()
                    .defaultValue("orc")
                    .withDescription("Specify the message format of data files.");

    public static final ConfigOption<String> MANIFEST_FORMAT =
            ConfigOptions.key("manifest.format")
                    .stringType()
                    .defaultValue("avro")
                    .withDescription("Specify the message format of manifest files.");

    public static final ConfigOption<MemorySize> MANIFEST_TARGET_FILE_SIZE =
            ConfigOptions.key("manifest.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription("Suggested file size of a manifest file.");

    public static final ConfigOption<Integer> MANIFEST_MERGE_MIN_COUNT =
            ConfigOptions.key("manifest.merge-min-count")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "To avoid frequent manifest merges, this parameter specifies the minimum number "
                                    + "of ManifestFileMeta to merge.");

    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "The default partition name in case the dynamic partition"
                                    + " column value is null/empty string.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MIN =
            ConfigOptions.key("snapshot.num-retained.min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The minimum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MAX =
            ConfigOptions.key("snapshot.num-retained.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            ConfigOptions.key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The maximum time of completed snapshots to retain.");

    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            ConfigOptions.key("continuous.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The discovery interval of continuous reading.");

    public static final ConfigOption<MergeEngine> MERGE_ENGINE =
            ConfigOptions.key("merge-engine")
                    .enumType(MergeEngine.class)
                    .defaultValue(MergeEngine.DEDUPLICATE)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the merge engine for table with primary key.")
                                    .linebreak()
                                    .list(
                                            formatEnumOption(MergeEngine.DEDUPLICATE),
                                            formatEnumOption(MergeEngine.PARTIAL_UPDATE))
                                    .build());

    public static final ConfigOption<WriteMode> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.CHANGE_LOG)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the write mode for table.")
                                    .linebreak()
                                    .list(formatEnumOption(WriteMode.APPEND_ONLY))
                                    .list(formatEnumOption(WriteMode.CHANGE_LOG))
                                    .build());

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_TARGET_SIZE =
            ConfigOptions.key("source.split.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a source split when scanning a bucket.");

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_OPEN_FILE_COST =
            ConfigOptions.key("source.split.open-file-cost")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription(
                            "Open file cost of a source file. It is used to avoid reading"
                                    + " too many files with a source split, which can be very slow.");

    private final Configuration options;

    public static Set<ConfigOption<?>> allOptions() {
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        allOptions.add(BUCKET);
        allOptions.add(PATH);
        allOptions.add(FILE_FORMAT);
        allOptions.add(MANIFEST_FORMAT);
        allOptions.add(MANIFEST_TARGET_FILE_SIZE);
        allOptions.add(MANIFEST_MERGE_MIN_COUNT);
        allOptions.add(PARTITION_DEFAULT_NAME);
        allOptions.add(SNAPSHOT_NUM_RETAINED_MIN);
        allOptions.add(SNAPSHOT_NUM_RETAINED_MAX);
        allOptions.add(SNAPSHOT_TIME_RETAINED);
        allOptions.add(CONTINUOUS_DISCOVERY_INTERVAL);
        allOptions.add(MERGE_ENGINE);
        allOptions.add(WRITE_MODE);
        allOptions.add(SOURCE_SPLIT_TARGET_SIZE);
        allOptions.add(SOURCE_SPLIT_OPEN_FILE_COST);
        return allOptions;
    }

    public FileStoreOptions(Map<String, String> options) {
        this(Configuration.fromMap(options));
    }

    public FileStoreOptions(Configuration options) {
        this.options = options;
        // TODO validate all keys
        Preconditions.checkArgument(
                snapshotNumRetainMin() > 0,
                SNAPSHOT_NUM_RETAINED_MIN.key() + " should be at least 1");
        Preconditions.checkArgument(
                snapshotNumRetainMin() <= snapshotNumRetainMax(),
                SNAPSHOT_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + SNAPSHOT_NUM_RETAINED_MAX.key());
    }

    public int bucket() {
        return options.get(BUCKET);
    }

    public Path path() {
        return path(options.toMap());
    }

    public static Path path(Map<String, String> options) {
        return new Path(options.get(PATH.key()));
    }

    public static Path path(Configuration options) {
        return new Path(options.get(PATH));
    }

    public static String relativeTablePath(ObjectIdentifier tableIdentifier) {
        return String.format(
                "%s.catalog/%s.db/%s",
                tableIdentifier.getCatalogName(),
                tableIdentifier.getDatabaseName(),
                tableIdentifier.getObjectName());
    }

    public FileFormat fileFormat() {
        return FileFormat.fromTableOptions(
                Thread.currentThread().getContextClassLoader(), options, FILE_FORMAT);
    }

    public FileFormat manifestFormat() {
        return FileFormat.fromTableOptions(
                Thread.currentThread().getContextClassLoader(), options, MANIFEST_FORMAT);
    }

    public MemorySize manifestTargetSize() {
        return options.get(MANIFEST_TARGET_FILE_SIZE);
    }

    public String partitionDefaultName() {
        return options.get(PARTITION_DEFAULT_NAME);
    }

    public MergeTreeOptions mergeTreeOptions() {
        return new MergeTreeOptions(options);
    }

    public int snapshotNumRetainMin() {
        return options.get(SNAPSHOT_NUM_RETAINED_MIN);
    }

    public int snapshotNumRetainMax() {
        return options.get(SNAPSHOT_NUM_RETAINED_MAX);
    }

    public Duration snapshotTimeRetain() {
        return options.get(SNAPSHOT_TIME_RETAINED);
    }

    public int manifestMergeMinCount() {
        return options.get(MANIFEST_MERGE_MIN_COUNT);
    }

    public long splitTargetSize() {
        return options.get(SOURCE_SPLIT_TARGET_SIZE).getBytes();
    }

    public long splitOpenFileCost() {
        return options.get(SOURCE_SPLIT_OPEN_FILE_COST).getBytes();
    }

    /** Specifies the merge engine for table with primary key. */
    public enum MergeEngine implements DescribedEnum {
        DEDUPLICATE("deduplicate", "De-duplicate and keep the last row."),

        PARTIAL_UPDATE("partial-update", "Partial update non-null fields.");

        private final String value;
        private final String description;

        MergeEngine(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }
}

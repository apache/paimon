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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Options for {@link FileStore}. */
public class FileStoreOptions implements Serializable {

    public static final String TABLE_STORE_PREFIX = "table-store.";

    public static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Bucket number for file store.");

    public static final ConfigOption<String> FILE_PATH =
            ConfigOptions.key("file.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of the table store in the filesystem.");

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

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED =
            ConfigOptions.key("snapshot.num-retained")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            ConfigOptions.key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofDays(1))
                    .withDescription("The maximum time of completed snapshots to retain.");

    private final Configuration options;

    public static Set<ConfigOption<?>> allOptions() {
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        allOptions.add(BUCKET);
        allOptions.add(FILE_PATH);
        allOptions.add(FILE_FORMAT);
        allOptions.add(MANIFEST_FORMAT);
        allOptions.add(MANIFEST_TARGET_FILE_SIZE);
        allOptions.add(MANIFEST_MERGE_MIN_COUNT);
        allOptions.add(PARTITION_DEFAULT_NAME);
        allOptions.add(SNAPSHOT_NUM_RETAINED);
        allOptions.add(SNAPSHOT_TIME_RETAINED);
        return allOptions;
    }

    public FileStoreOptions(Configuration options) {
        this.options = options;
        // TODO validate all keys
    }

    public int bucket() {
        return options.get(BUCKET);
    }

    public Path path() {
        return new Path(options.get(FILE_PATH));
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

    public int snapshotNumRetain() {
        return options.get(SNAPSHOT_NUM_RETAINED);
    }

    public Duration snapshotTimeRetain() {
        return options.get(SNAPSHOT_TIME_RETAINED);
    }

    public int manifestMergeMinCount() {
        return options.get(MANIFEST_MERGE_MIN_COUNT);
    }
}

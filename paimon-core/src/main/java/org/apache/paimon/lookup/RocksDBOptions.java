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

package org.apache.paimon.lookup;

import org.apache.paimon.annotation.Documentation;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.Description;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.TableFormatConfig;

import java.time.Duration;

import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.description.LinkElement.link;
import static org.apache.paimon.options.description.TextElement.code;
import static org.rocksdb.CompactionStyle.FIFO;
import static org.rocksdb.CompactionStyle.LEVEL;
import static org.rocksdb.CompactionStyle.NONE;
import static org.rocksdb.CompactionStyle.UNIVERSAL;
import static org.rocksdb.CompressionType.LZ4_COMPRESSION;
import static org.rocksdb.InfoLogLevel.INFO_LEVEL;

/** Options for rocksdb. Copied from flink {@code RocksDBConfigurableOptions}. */
public class RocksDBOptions {

    public static final ConfigOption<Long> LOOKUP_CACHE_ROWS =
            key("lookup.cache-rows")
                    .longType()
                    .defaultValue(10_000L)
                    .withDescription("The maximum number of rows to store in the cache.");

    public static final ConfigOption<Duration> LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL =
            key("lookup.continuous.discovery-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The discovery interval of lookup continuous reading. This is used as an SQL hint. If it's not configured, the lookup function will fallback to 'continuous.discovery-interval'.");

    // --------------------------------------------------------------------------
    // Provided configurable DBOptions within Flink
    // --------------------------------------------------------------------------

    public static final ConfigOption<Integer> MAX_BACKGROUND_THREADS =
            key("rocksdb.thread.num")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The maximum number of concurrent background flush and compaction jobs (per stateful operator). "
                                    + "The default value is '2'.");

    public static final ConfigOption<Integer> MAX_OPEN_FILES =
            key("rocksdb.files.open")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The maximum number of open files (per stateful operator) that can be used by the DB, '-1' means no limit. "
                                    + "The default value is '-1'.");

    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<MemorySize> LOG_MAX_FILE_SIZE =
            key("rocksdb.log.max-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("25mb"))
                    .withDescription(
                            "The maximum size of RocksDB's file used for information logging. "
                                    + "If the log files becomes larger than this, a new file will be created. "
                                    + "If 0, all logs will be written to one log file. "
                                    + "The default maximum file size is '25MB'. ");

    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<Integer> LOG_FILE_NUM =
            key("rocksdb.log.file-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The maximum number of files RocksDB should keep for information logging (Default setting: 4).");

    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> LOG_DIR =
            key("rocksdb.log.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The directory for RocksDB's information logging files. "
                                    + "If empty (Flink default setting), log files will be in the same directory as the Flink log. "
                                    + "If non-empty, this directory will be used and the data directory's absolute path will be used as the prefix of the log file name.");

    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<InfoLogLevel> LOG_LEVEL =
            key("rocksdb.log.level")
                    .enumType(InfoLogLevel.class)
                    .defaultValue(INFO_LEVEL)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The specified information logging level for RocksDB. "
                                                    + "If unset, Flink will use %s.",
                                            code(INFO_LEVEL.name()))
                                    .linebreak()
                                    .text(
                                            "Note: RocksDB info logs will not be written to the TaskManager logs and there "
                                                    + "is no rolling strategy, unless you configure %s, %s, and %s accordingly. "
                                                    + "Without a rolling strategy, long-running tasks may lead to uncontrolled "
                                                    + "disk space usage if configured with increased log levels!",
                                            code(LOG_DIR.key()),
                                            code(LOG_MAX_FILE_SIZE.key()),
                                            code(LOG_FILE_NUM.key()))
                                    .linebreak()
                                    .text(
                                            "There is no need to modify the RocksDB log level, unless for troubleshooting RocksDB.")
                                    .build());

    // --------------------------------------------------------------------------
    // Provided configurable ColumnFamilyOptions within Flink
    // --------------------------------------------------------------------------

    public static final ConfigOption<CompressionType> COMPRESSION_TYPE =
            key("rocksdb.compression.type")
                    .enumType(CompressionType.class)
                    .defaultValue(LZ4_COMPRESSION)
                    .withDescription("The compression type.");

    public static final ConfigOption<CompactionStyle> COMPACTION_STYLE =
            key("rocksdb.compaction.style")
                    .enumType(CompactionStyle.class)
                    .defaultValue(LEVEL)
                    .withDescription(
                            String.format(
                                    "The specified compaction style for DB. Candidate compaction style is %s, %s, %s or %s, "
                                            + "and Flink chooses '%s' as default style.",
                                    LEVEL.name(),
                                    FIFO.name(),
                                    UNIVERSAL.name(),
                                    NONE.name(),
                                    LEVEL.name()));

    public static final ConfigOption<Boolean> USE_DYNAMIC_LEVEL_SIZE =
            key("rocksdb.compaction.level.use-dynamic-size")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If true, RocksDB will pick target size of each level dynamically. From an empty DB, ")
                                    .text(
                                            "RocksDB would make last level the base level, which means merging L0 data into the last level, ")
                                    .text(
                                            "until it exceeds max_bytes_for_level_base. And then repeat this process for second last level and so on. ")
                                    .text("The default value is 'false'. ")
                                    .text(
                                            "For more information, please refer to %s",
                                            link(
                                                    "https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#level_compaction_dynamic_level_bytes-is-true",
                                                    "RocksDB's doc."))
                                    .build());

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE_BASE =
            key("rocksdb.compaction.level.target-file-size-base")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The target file size for compaction, which determines a level-1 file size. "
                                    + "The default value is '64MB'.");

    public static final ConfigOption<MemorySize> MAX_SIZE_LEVEL_BASE =
            key("rocksdb.compaction.level.max-size-level-base")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256mb"))
                    .withDescription(
                            "The upper-bound of the total size of level base files in bytes. "
                                    + "The default value is '256MB'.");

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("rocksdb.writebuffer.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "The amount of data built up in memory (backed by an unsorted log on disk) "
                                    + "before converting to a sorted on-disk files. The default writebuffer size is '64MB'.");

    public static final ConfigOption<Integer> MAX_WRITE_BUFFER_NUMBER =
            key("rocksdb.writebuffer.count")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The maximum number of write buffers that are built up in memory. "
                                    + "The default value is '2'.");

    public static final ConfigOption<Integer> MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
            key("rocksdb.writebuffer.number-to-merge")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The minimum number of write buffers that will be merged together before writing to storage. "
                                    + "The default value is '1'.");

    public static final ConfigOption<MemorySize> BLOCK_SIZE =
            key("rocksdb.block.blocksize")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "The approximate size (in bytes) of user data packed per block. "
                                    + "The default blocksize is '4KB'.");

    public static final ConfigOption<MemorySize> METADATA_BLOCK_SIZE =
            key("rocksdb.block.metadata-blocksize")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription(
                            "Approximate size of partitioned metadata packed per block. "
                                    + "Currently applied to indexes block when partitioned index/filters option is enabled. "
                                    + "The default blocksize is '4KB'.");

    public static final ConfigOption<MemorySize> BLOCK_CACHE_SIZE =
            key("rocksdb.block.cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128mb"))
                    .withDescription("The amount of the cache for data blocks in RocksDB.");

    public static final ConfigOption<Boolean> USE_BLOOM_FILTER =
            key("rocksdb.use-bloom-filter")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, every newly created SST file will contain a Bloom filter. "
                                    + "It is disabled by default.");

    public static final ConfigOption<Double> BLOOM_FILTER_BITS_PER_KEY =
            key("rocksdb.bloom-filter.bits-per-key")
                    .doubleType()
                    .defaultValue(10.0)
                    .withDescription(
                            "Bits per key that bloom filter will use, this only take effect when bloom filter is used. "
                                    + "The default value is 10.0.");

    public static final ConfigOption<Boolean> BLOOM_FILTER_BLOCK_BASED_MODE =
            key("rocksdb.bloom-filter.block-based-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, RocksDB will use block-based filter instead of full filter, this only take effect when bloom filter is used. "
                                    + "The default value is 'false'.");

    public static DBOptions createDBOptions(DBOptions currentOptions, Options options) {
        currentOptions.setMaxBackgroundJobs(options.get(MAX_BACKGROUND_THREADS));
        currentOptions.setMaxOpenFiles(options.get(MAX_OPEN_FILES));
        currentOptions.setInfoLogLevel(options.get(LOG_LEVEL));

        String logDir = options.get(LOG_DIR);
        if (logDir != null && !logDir.isEmpty()) {
            currentOptions.setDbLogDir(logDir);
        }

        currentOptions.setMaxLogFileSize(options.get(LOG_MAX_FILE_SIZE).getBytes());
        currentOptions.setKeepLogFileNum(options.get(LOG_FILE_NUM));
        return currentOptions;
    }

    public static ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Options options) {
        currentOptions.setCompressionType(options.get(COMPRESSION_TYPE));
        currentOptions.setCompactionStyle(options.get(COMPACTION_STYLE));
        currentOptions.setLevelCompactionDynamicLevelBytes(options.get(USE_DYNAMIC_LEVEL_SIZE));
        currentOptions.setTargetFileSizeBase(options.get(TARGET_FILE_SIZE_BASE).getBytes());
        currentOptions.setMaxBytesForLevelBase(options.get(MAX_SIZE_LEVEL_BASE).getBytes());
        currentOptions.setWriteBufferSize(options.get(WRITE_BUFFER_SIZE).getBytes());
        currentOptions.setMaxWriteBufferNumber(options.get(MAX_WRITE_BUFFER_NUMBER));
        currentOptions.setMinWriteBufferNumberToMerge(
                options.get(MIN_WRITE_BUFFER_NUMBER_TO_MERGE));

        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

        BlockBasedTableConfig blockBasedTableConfig;
        if (tableFormatConfig == null) {
            blockBasedTableConfig = new BlockBasedTableConfig();
        } else {
            if (tableFormatConfig instanceof PlainTableConfig) {
                // if the table format config is PlainTableConfig, we just return current
                // column-family options
                return currentOptions;
            } else {
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
        }

        blockBasedTableConfig.setBlockSize(options.get(BLOCK_SIZE).getBytes());
        blockBasedTableConfig.setMetadataBlockSize(options.get(METADATA_BLOCK_SIZE).getBytes());
        blockBasedTableConfig.setBlockCacheSize(options.get(BLOCK_CACHE_SIZE).getBytes());

        if (options.get(USE_BLOOM_FILTER)) {
            double bitsPerKey = options.get(BLOOM_FILTER_BITS_PER_KEY);
            boolean blockBasedMode = options.get(BLOOM_FILTER_BLOCK_BASED_MODE);
            BloomFilter bloomFilter = new BloomFilter(bitsPerKey, blockBasedMode);
            blockBasedTableConfig.setFilterPolicy(bloomFilter);
        }

        return currentOptions.setTableFormatConfig(blockBasedTableConfig);
    }
}

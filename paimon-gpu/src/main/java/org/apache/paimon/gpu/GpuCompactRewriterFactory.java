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

package org.apache.paimon.gpu;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.CompactRewriterFactory;
import org.apache.paimon.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Installs the device rewriter on the buckets of a table it supports; every other bucket, and every
 * compaction the device cannot take, keeps Paimon's rewriter.
 *
 * <pre>{@code
 * try (GpuCompactRewriterFactory gpu = new GpuCompactRewriterFactory(table)) {
 *     write.withCompactRewriterFactory(gpu);
 *     ...
 * }
 * }</pre>
 *
 * <p>The table option {@code gpu.compaction.max-batch-rows} (default 64M) caps the rows of one
 * device batch; a compaction with a larger section stays on the CPU.
 */
public final class GpuCompactRewriterFactory implements CompactRewriterFactory, AutoCloseable {

    /** Table option: the most rows one device batch reads. */
    public static final String MAX_BATCH_ROWS = "gpu.compaction.max-batch-rows";

    private static final Logger LOG = LoggerFactory.getLogger(GpuCompactRewriterFactory.class);

    private final FileStoreTable table;
    private final CoreOptions options;
    private final long maxBatchRows;
    private final GpuMergeTreeCompactRewriter.Stats stats = new GpuMergeTreeCompactRewriter.Stats();
    private DeviceCompactor device;
    private KvLayout layout;
    private Optional<SimpleStatsExtractor> extractor;
    private boolean checked;

    public GpuCompactRewriterFactory(FileStoreTable table) {
        this.table = table;
        this.options = table.coreOptions();
        this.maxBatchRows =
                Long.parseLong(
                        table.options().getOrDefault(MAX_BATCH_ROWS, Long.toString(64L << 20)));
    }

    @Override
    public synchronized CompactRewriter create(
            BinaryRow partition, int bucket, CompactRewriter defaultRewriter) {
        if (defaultRewriter.getClass() != MergeTreeCompactRewriter.class) {
            stats.cpu("rewriter " + defaultRewriter.getClass().getSimpleName());
            return defaultRewriter;
        }
        KeyValueFileWriterFactory writerFactory =
                ((MergeTreeCompactRewriter) defaultRewriter).writerFactory();
        if (!checked) {
            checked = true;
            StringBuilder why = new StringBuilder();
            layout = KvLayout.of(writerFactory.keyType(), writerFactory.valueType(), options, why);
            if (layout == null) {
                LOG.info("Table {} compacts on the CPU: {}", table.name(), why);
                stats.cpu(why.toString());
            } else {
                RowType writeRowType = KeyValue.schema(layout.keyType, layout.valueType);
                extractor =
                        FileFormat.fromIdentifier("parquet", options.toConfiguration())
                                .createStatsExtractor(
                                        writeRowType,
                                        StatsCollectorFactories.createStatsFactories(
                                                options.statsMode(),
                                                options,
                                                writeRowType.getFieldNames()));
                device = new DeviceCompactor(layout);
            }
        }
        if (layout == null || device == null) {
            // Unsupported table, or the factory is closed.
            return defaultRewriter;
        }
        return new GpuMergeTreeCompactRewriter(
                defaultRewriter,
                writerFactory,
                layout,
                device,
                options,
                table.schema().id(),
                extractor,
                maxBatchRows,
                stats);
    }

    /** What the rewriters made by this factory did so far. */
    public String stats() {
        return stats.toString();
    }

    @Override
    public synchronized void close() {
        if (device != null) {
            device.close();
            device = null;
        }
    }
}

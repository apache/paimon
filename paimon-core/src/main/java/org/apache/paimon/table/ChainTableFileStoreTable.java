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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Map;

/**
 * Chain-table-aware extension of {@link FallbackReadFileStoreTable}. Inherits the batch read
 * behavior (partition-level fallback between the current branch and {@link ChainGroupReadTable}),
 * and additionally overrides {@link #newStreamScan()} to return a chain-aware {@link
 * ChainTableStreamScan} that performs a partition-level full load followed by incremental
 * delta-only streaming.
 */
public class ChainTableFileStoreTable extends FallbackReadFileStoreTable {

    public ChainTableFileStoreTable(FileStoreTable wrapped, FileStoreTable other) {
        super(wrapped, other, true);
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        CoreOptions coreOptions = wrapped.coreOptions();

        StartupMode effectiveMode = coreOptions.startupMode();
        boolean hasConsumer = coreOptions.consumerId() != null;
        if (effectiveMode != StartupMode.LATEST_FULL || hasConsumer) {
            String reason = describeUnsupportedMode(coreOptions, effectiveMode, hasConsumer);
            throw new UnsupportedOperationException(
                    "Chain table streaming read does not support startup mode '"
                            + reason
                            + "'. "
                            + "Chain table streaming only supports the default 'latest-full' mode, which first "
                            + "produces a partition-level full result and then continuously reads incremental "
                            + "data from the delta branch.\n"
                            + "Suggestions:\n"
                            + "  - To use chain table streaming: remove the explicit scan mode/position settings "
                            + "so that the default 'latest-full' mode is used.\n"
                            + "  - To use standard streaming read without chain table logic: read from a "
                            + "specific branch table (e.g., 't$branch_delta') instead of the main table.");
        }

        // Inherited other() returns the ChainGroupReadTable directly.
        ChainGroupReadTable chainGroupReadTable = (ChainGroupReadTable) other();

        return new ChainTableStreamScan(chainGroupReadTable);
    }

    private static String describeUnsupportedMode(
            CoreOptions coreOptions, StartupMode effectiveMode, boolean hasConsumer) {
        if (hasConsumer) {
            return "consumer mode (consumer-id='" + coreOptions.consumerId() + "')";
        }
        switch (effectiveMode) {
            case LATEST:
                return "scan.mode=latest";
            case FROM_SNAPSHOT:
                if (coreOptions.scanSnapshotId() != null) {
                    return "scan.snapshot-id=" + coreOptions.scanSnapshotId();
                }
                if (coreOptions.scanTagName() != null) {
                    return "scan.tag-name=" + coreOptions.scanTagName();
                }
                if (coreOptions.scanWatermark() != null) {
                    return "scan.watermark=" + coreOptions.scanWatermark();
                }
                return "from-snapshot";
            case FROM_TIMESTAMP:
                if (coreOptions.scanTimestampMills() != null) {
                    return "scan.timestamp-millis=" + coreOptions.scanTimestampMills();
                }
                if (coreOptions.scanTimestamp() != null) {
                    return "scan.timestamp=" + coreOptions.scanTimestamp();
                }
                return "from-timestamp";
            default:
                return effectiveMode.name().toLowerCase().replace('_', '-');
        }
    }

    @Override
    public InnerTableRead newRead() {
        return new ChainTableRead();
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainTableFileStoreTable(
                wrapped.copy(dynamicOptions), other().copy(rewriteOtherOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainTableFileStoreTable(
                wrapped.copy(newTableSchema),
                other().copy(newTableSchema.copy(rewriteOtherOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainTableFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                other().copyWithoutTimeTravel(rewriteOtherOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainTableFileStoreTable(
                wrapped.copyWithLatestSchema(), other().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainTableFileStoreTable(switchWrappedToBranch(branchName), other());
    }

    /**
     * Chain-aware read implementation that pairs with {@link ChainTableStreamScan}. Routes splits
     * based on type:
     *
     * <ul>
     *   <li><b>ChainSplit / DataSplit:</b> Streaming read splits. Routed to {@link
     *       ChainGroupReadTable}'s read which uses {@link
     *       org.apache.paimon.io.ChainKeyValueFileReaderFactory} (both branch schemas for
     *       ChainSplit) or delta branch read (DataSplit with correct schema).
     *   <li><b>FallbackSplit:</b> Batch read fallback splits. Routed to inherited {@link
     *       FallbackReadFileStoreTable} read for partition-level fallback logic.
     * </ul>
     */
    private class ChainTableRead implements InnerTableRead {

        private final InnerTableRead chainGroupRead;
        private final InnerTableRead fallbackRead;

        private ChainTableRead() {
            this.chainGroupRead = other().newRead();
            this.fallbackRead = ChainTableFileStoreTable.super.newRead();
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            chainGroupRead.withFilter(predicate);
            fallbackRead.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            chainGroupRead.withReadType(readType);
            fallbackRead.withReadType(readType);
            return this;
        }

        @Override
        public InnerTableRead forceKeepDelete() {
            chainGroupRead.forceKeepDelete();
            fallbackRead.forceKeepDelete();
            return this;
        }

        @Override
        public TableRead executeFilter() {
            chainGroupRead.executeFilter();
            fallbackRead.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            chainGroupRead.withIOManager(ioManager);
            fallbackRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (split instanceof FallbackSplit) {
                // FallbackSplit (including FallbackDataSplit): use inherited fallback read logic
                return fallbackRead.createReader(split);
            }
            if (split instanceof ChainSplit || split instanceof DataSplit) {
                return chainGroupRead.createReader(split);
            }
            // Other split types: use inherited fallback read logic
            return fallbackRead.createReader(split);
        }
    }
}

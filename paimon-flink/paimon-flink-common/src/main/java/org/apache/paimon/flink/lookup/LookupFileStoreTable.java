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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.options.description.TextElement.text;

/** {@link FileStoreTable} for lookup table. */
public class LookupFileStoreTable extends DelegatedFileStoreTable {

    private static final long serialVersionUID = 1L;

    private final LookupStreamScanMode lookupScanMode;

    public LookupFileStoreTable(FileStoreTable wrapped, List<String> joinKeys) {
        super(wrapped);
        this.lookupScanMode = lookupStreamScanMode(wrapped, joinKeys);
    }

    public LookupFileStoreTable(FileStoreTable wrapped, LookupStreamScanMode lookupScanMode) {
        super(wrapped);
        this.lookupScanMode = lookupScanMode;
    }

    @Override
    public InnerTableRead newRead() {
        switch (lookupScanMode) {
            case CHANGELOG:
            case FILE_MONITOR:
                return wrapped.newRead();
            case COMPACT_DELTA_MONITOR:
                return new LookupCompactDiffRead(
                        ((KeyValueFileStore) wrapped.store()).newRead(), wrapped.schema());
            default:
                throw new UnsupportedOperationException(
                        "Unknown lookup stream scan mode: " + lookupScanMode.name());
        }
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new LookupDataTableScan(
                wrapped.coreOptions(),
                wrapped.newSnapshotReader(),
                wrapped.snapshotManager(),
                wrapped.supportStreamingReadOverwrite(),
                DefaultValueAssigner.create(wrapped.schema()),
                lookupScanMode);
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new LookupFileStoreTable(wrapped.copy(dynamicOptions), lookupScanMode);
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new LookupFileStoreTable(wrapped.copy(newTableSchema), lookupScanMode);
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new LookupFileStoreTable(wrapped.copy(dynamicOptions), lookupScanMode);
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new LookupFileStoreTable(wrapped.copyWithLatestSchema(), lookupScanMode);
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        wrapped.switchToBranch(branchName);
        return this;
    }

    @Override
    public ExternalPathProvider externalPathProvider() {
        return wrapped.externalPathProvider();
    }

    private LookupStreamScanMode lookupStreamScanMode(FileStoreTable table, List<String> joinKeys) {
        Options options = Options.fromMap(table.options());
        if (options.get(LOOKUP_CACHE_MODE) == FlinkConnectorOptions.LookupCacheMode.AUTO
                && new HashSet<>(table.primaryKeys()).equals(new HashSet<>(joinKeys))) {
            return LookupStreamScanMode.FILE_MONITOR;
        } else if (table.primaryKeys().size() > 0
                && options.get(CHANGELOG_PRODUCER) == CoreOptions.ChangelogProducer.NONE
                && TableScanUtils.supportCompactDiffStreamingReading(table)) {
            return LookupStreamScanMode.COMPACT_DELTA_MONITOR;
        } else {
            return LookupStreamScanMode.CHANGELOG;
        }
    }

    /** Inner stream scan mode for lookup table. */
    public enum LookupStreamScanMode implements DescribedEnum {
        CHANGELOG("changelog", "Streaming reading based on changelog or delta data files."),
        FILE_MONITOR("file-monitor", "Monitor data file changes."),
        COMPACT_DELTA_MONITOR(
                "compact-delta-monitor",
                "Streaming reading based on data changes before and after compaction.");

        private final String value;
        private final String description;

        LookupStreamScanMode(String value, String description) {
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

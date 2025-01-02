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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.system.CompactBucketsTable;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.compactOptions;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * The operator that reads the Tuple2<{@link Split}, String> received from the preceding {@link
 * CombinedAwareBatchSource} or {@link CombinedAwareStreamingSource}. Contrary to the {@link
 * CombinedCompactorSource} which has a parallelism of 1, this operator can have DOP > 1.
 */
public class MultiTablesReadOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<Tuple2<Split, String>, RowData> {

    private static final long serialVersionUID = 1L;

    private final CatalogLoader catalogLoader;
    private final boolean isStreaming;

    private Duration partitionIdleTime = null;

    public MultiTablesReadOperator(CatalogLoader catalogLoader, boolean isStreaming) {
        this.catalogLoader = catalogLoader;
        this.isStreaming = isStreaming;
    }

    public MultiTablesReadOperator(
            CatalogLoader catalogLoader, boolean isStreaming, Duration partitionIdleTime) {
        this.catalogLoader = catalogLoader;
        this.isStreaming = isStreaming;
        this.partitionIdleTime = partitionIdleTime;
    }

    private transient Catalog catalog;
    private transient IOManager ioManager;
    private transient Map<Identifier, CompactBucketsTable> tablesMap;
    private transient Map<Identifier, TableRead> readsMap;
    private transient StreamRecord<RowData> reuseRecord;
    private transient FlinkRowData reuseRow;

    @Override
    public void open() throws Exception {
        super.open();
        ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        tablesMap = new HashMap<>();
        readsMap = new HashMap<>();
        catalog = catalogLoader.load();

        this.reuseRow = new FlinkRowData(null);
        this.reuseRecord = new StreamRecord<>(reuseRow);

        if (isStreaming) {
            Preconditions.checkArgument(
                    partitionIdleTime == null, "Streaming mode does not support partitionIdleTime");
        }
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Split, String>> record) throws Exception {
        Identifier identifier = Identifier.fromString(record.getValue().f1);
        TableRead read = getTableRead(identifier);
        Map<BinaryRow, Long> partitionInfo = getPartitionInfo(tablesMap.get(identifier));
        try (CloseableIterator<InternalRow> iterator =
                read.createReader(record.getValue().f0).toCloseableIterator()) {
            if (partitionIdleTime == null) {
                while (iterator.hasNext()) {
                    reuseRow.replace(iterator.next());
                    output.collect(reuseRecord);
                }
            } else {
                while (iterator.hasNext()) {
                    InternalRow row = iterator.next();
                    if (checkIsHistoryPartition(row, partitionInfo)) {
                        reuseRow.replace(row);
                        output.collect(reuseRecord);
                    }
                }
            }
        }
    }

    private TableRead getTableRead(Identifier tableId) {
        CompactBucketsTable table = tablesMap.get(tableId);
        if (table == null) {
            try {
                Table newTable = catalog.getTable(tableId);
                Preconditions.checkArgument(
                        newTable instanceof FileStoreTable,
                        "Only FileStoreTable supports compact action. The table type is '%s'.",
                        newTable.getClass().getName());
                table =
                        new CompactBucketsTable(
                                        (FileStoreTable) newTable,
                                        isStreaming,
                                        tableId.getDatabaseName())
                                .copy(compactOptions(isStreaming));
                tablesMap.put(tableId, table);
                readsMap.put(tableId, table.newReadBuilder().newRead().withIOManager(ioManager));
            } catch (Catalog.TableNotExistException e) {
                LOG.error(String.format("table: %s not found.", tableId.getFullName()));
            }
        }

        return readsMap.get(tableId);
    }

    private Map<BinaryRow, Long> getPartitionInfo(CompactBucketsTable table) {
        List<PartitionEntry> partitions = table.newSnapshotReader().partitionEntries();

        return partitions.stream()
                .collect(
                        Collectors.toMap(
                                PartitionEntry::partition, PartitionEntry::lastFileCreationTime));
    }

    private boolean checkIsHistoryPartition(InternalRow row, Map<BinaryRow, Long> partitionInfo) {
        BinaryRow partition = deserializeBinaryRow(row.getBinary(1));
        long historyMilli =
                LocalDateTime.now()
                        .minus(partitionIdleTime)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
        return partitionInfo.get(partition) <= historyMilli;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ioManager != null) {
            ioManager.close();
        }
        if (catalog != null) {
            catalog.close();
        }
    }
}

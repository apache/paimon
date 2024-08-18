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

import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The operator is used for historical partition compaction. It reads {@link
 * MultiTableUnawareAppendCompactionTask} received from the preceding {@link
 * CombinedUnawareBatchSourceFunction} and filter partitions which is not historical.
 */
public class MultiUnawareTablesReadOperator
        extends AbstractStreamOperator<MultiTableUnawareAppendCompactionTask>
        implements OneInputStreamOperator<
                MultiTableUnawareAppendCompactionTask, MultiTableUnawareAppendCompactionTask> {
    private static final long serialVersionUID = 1L;

    private final Catalog.Loader catalogLoader;

    private final Duration partitionIdleTime;

    public MultiUnawareTablesReadOperator(
            Catalog.Loader catalogLoader, Duration partitionIdleTime) {
        this.catalogLoader = catalogLoader;
        this.partitionIdleTime = partitionIdleTime;
    }

    private transient Catalog catalog;
    private transient Map<Identifier, FileStoreTable> tablesMap;

    @Override
    public void open() throws Exception {
        super.open();
        tablesMap = new HashMap<>();
        catalog = catalogLoader.load();
    }

    @Override
    public void processElement(StreamRecord<MultiTableUnawareAppendCompactionTask> record) {
        Identifier identifier = record.getValue().tableIdentifier();
        BinaryRow partition = record.getValue().partition();
        FileStoreTable table = getTable(identifier);
        Map<BinaryRow, Long> partitionInfo = getPartitionInfo(table);
        if (checkIsHistoryPartition(partition, partitionInfo)) {
            output.collect(record);
        }
    }

    private FileStoreTable getTable(Identifier tableId) {
        FileStoreTable table = tablesMap.get(tableId);
        if (table == null) {
            try {
                Table newTable = catalog.getTable(tableId);
                Preconditions.checkArgument(
                        newTable instanceof FileStoreTable,
                        "Only FileStoreTable supports compact action. The table type is '%s'.",
                        newTable.getClass().getName());
                table = (FileStoreTable) newTable;
                tablesMap.put(tableId, table);
            } catch (Catalog.TableNotExistException e) {
                LOG.error(String.format("table: %s not found.", tableId.getFullName()));
            }
        }

        return table;
    }

    private Map<BinaryRow, Long> getPartitionInfo(FileStoreTable table) {
        List<PartitionEntry> partitions = table.newSnapshotReader().partitionEntries();
        return partitions.stream()
                .collect(
                        Collectors.toMap(
                                PartitionEntry::partition, PartitionEntry::lastFileCreationTime));
    }

    private boolean checkIsHistoryPartition(
            BinaryRow partition, Map<BinaryRow, Long> partitionInfo) {
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
        if (catalog != null) {
            catalog.close();
        }
    }
}

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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** A dedicated operator for manual triggered compaction. */
public class StoreCompactOperator extends PrepareCommitOperator {

    private static final Logger LOG = LoggerFactory.getLogger(StoreCompactOperator.class);

    private final FileStoreTable table;
    private final String commitUser;
    @Nullable private final Map<String, String> compactPartitionSpec;

    private TableScan scan;
    private TableWrite write;

    public StoreCompactOperator(
            FileStoreTable table,
            String commitUser,
            @Nullable Map<String, String> compactPartitionSpec) {
        this.table = table;
        this.commitUser = commitUser;
        this.compactPartitionSpec = compactPartitionSpec;
    }

    @Override
    public void open() throws Exception {
        super.open();

        scan = table.newScan();
        if (compactPartitionSpec != null) {
            scan.withFilter(
                    PredicateConverter.fromMap(
                            compactPartitionSpec, table.schema().logicalPartitionType()));
        }

        write = table.newWrite(commitUser);
    }

    @Override
    protected List<Committable> prepareCommit(boolean endOfInput, long checkpointId)
            throws IOException {
        int task = getRuntimeContext().getIndexOfThisSubtask();
        int numTask = getRuntimeContext().getNumberOfParallelSubtasks();

        for (Split split : scan.plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            BinaryRowData partition = dataSplit.partition();
            int bucket = dataSplit.bucket();
            if (Math.abs(Objects.hash(partition, bucket)) % numTask != task) {
                continue;
            }

            if (LOG.isDebugEnabled()) {
                RowType partitionType = table.schema().logicalPartitionType();
                LOG.debug(
                        "Do compaction for partition {}, bucket {}",
                        FileStorePathFactory.getPartitionComputer(
                                        partitionType,
                                        FileStorePathFactory.PARTITION_DEFAULT_NAME.defaultValue())
                                .generatePartValues(partition),
                        bucket);
            }
            try {
                write.compact(partition, bucket);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            return write.prepareCommit(true, checkpointId).stream()
                    .map(c -> new Committable(checkpointId, Committable.Kind.FILE, c))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

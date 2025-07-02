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

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Dynamic partition for lookup. */
public abstract class DynamicPartitionLoader extends PartitionLoader {

    private static final long serialVersionUID = 2L;

    protected final Duration refreshInterval;

    protected transient Comparator<InternalRow> comparator;
    protected transient LocalDateTime lastRefresh;

    DynamicPartitionLoader(FileStoreTable table, Duration refreshInterval) {
        super(table);
        this.refreshInterval = refreshInterval;
    }

    @Override
    public void open() {
        super.open();

        RowType partitionType = table.rowType().project(table.partitionKeys());
        this.comparator = CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());
        this.lastRefresh = null;
    }

    protected abstract List<BinaryRow> getMaxPartitions();

    protected String partitionsToString(List<BinaryRow> partitions) {
        return partitions.stream()
                .map(
                        partition ->
                                InternalRowPartitionComputer.partToSimpleString(
                                        table.rowType().project(table.partitionKeys()),
                                        partition,
                                        "-",
                                        200))
                .collect(Collectors.joining(","));
    }
}

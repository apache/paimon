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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Dynamic partition for lookup. */
public class DynamicPartitionLoader extends PartitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionLoader.class);

    private static final long serialVersionUID = 2L;

    private final Duration refreshInterval;
    private final int maxPartitionNum;

    private transient Comparator<InternalRow> comparator;
    private transient LocalDateTime lastRefresh;

    DynamicPartitionLoader(FileStoreTable table, Duration refreshInterval, int maxPartitionNum) {
        super(table);
        this.refreshInterval = refreshInterval;
        this.maxPartitionNum = maxPartitionNum;
    }

    @Override
    public void open() {
        super.open();
        RowType partitionType = table.rowType().project(table.partitionKeys());
        this.comparator = CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());
    }

    @Override
    public boolean checkRefresh() {
        if (lastRefresh != null
                && !lastRefresh.plus(refreshInterval).isBefore(LocalDateTime.now())) {
            return false;
        }

        LOG.info(
                "DynamicPartitionLoader(maxPartitionNum={},table={}) refreshed after {} second(s), refreshing",
                maxPartitionNum,
                table.name(),
                refreshInterval.toMillis() / 1000);

        List<BinaryRow> newPartitions = getMaxPartitions();
        lastRefresh = LocalDateTime.now();

        if (newPartitions.size() != partitions.size()) {
            partitions = newPartitions;
            logNewPartitions();
            return true;
        } else {
            for (int i = 0; i < newPartitions.size(); i++) {
                if (comparator.compare(newPartitions.get(i), partitions.get(i)) != 0) {
                    partitions = newPartitions;
                    logNewPartitions();
                    return true;
                }
            }
            LOG.info(
                    "DynamicPartitionLoader(maxPartitionNum={},table={}) didn't find new partitions.",
                    maxPartitionNum,
                    table.name());
            return false;
        }
    }

    private void logNewPartitions() {
        String partitionsStr =
                partitions.stream()
                        .map(
                                partition ->
                                        InternalRowPartitionComputer.partToSimpleString(
                                                table.rowType().project(table.partitionKeys()),
                                                partition,
                                                "-",
                                                200))
                        .collect(Collectors.joining(","));
        LOG.info(
                "DynamicPartitionLoader(maxPartitionNum={},table={}) finds new partitions: {}.",
                maxPartitionNum,
                table.name(),
                partitionsStr);
    }

    private List<BinaryRow> getMaxPartitions() {
        List<BinaryRow> newPartitions =
                table.newReadBuilder().newScan().listPartitions().stream()
                        .sorted(comparator.reversed())
                        .collect(Collectors.toList());

        if (newPartitions.size() <= maxPartitionNum) {
            return newPartitions;
        } else {
            return newPartitions.subList(0, maxPartitionNum);
        }
    }
}

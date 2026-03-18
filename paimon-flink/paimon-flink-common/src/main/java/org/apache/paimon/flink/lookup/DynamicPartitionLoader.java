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
import org.apache.paimon.table.Table;
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
public abstract class DynamicPartitionLoader extends PartitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionLoader.class);

    private static final long serialVersionUID = 3L;

    protected final Duration refreshInterval;

    protected transient Comparator<InternalRow> comparator;
    protected transient LocalDateTime lastRefresh;

    DynamicPartitionLoader(Table table, Duration refreshInterval) {
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

    @Override
    public boolean checkRefresh() {
        if (lastRefresh != null
                && !lastRefresh.plus(refreshInterval).isBefore(LocalDateTime.now())) {
            return false;
        }

        LOG.info(
                "DynamicPartitionLoader(table={}) refreshed after {} second(s), refreshing",
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
            LOG.info("DynamicPartitionLoader(table={}) didn't find new partitions.", table.name());
            return false;
        }
    }

    public abstract List<BinaryRow> getMaxPartitions();

    private void logNewPartitions() {
        String partitionsStr = partitionsToString(partitions);

        LOG.info(
                "DynamicPartitionLoader(table={}) finds new partitions: {}.",
                table.name(),
                partitionsStr);
    }

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

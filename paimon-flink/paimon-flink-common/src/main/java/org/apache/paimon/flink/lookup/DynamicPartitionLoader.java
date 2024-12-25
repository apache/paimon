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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Dynamic partition for lookup. */
public class DynamicPartitionLoader implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String MAX_PT = "max_pt()";

    private static final String MAX_TWO_PT = "max_two_pt()";

    private final Table table;
    private final Duration refreshInterval;
    private final int maxPartitionNum;

    private Comparator<InternalRow> comparator;

    private LocalDateTime lastRefresh;
    private List<BinaryRow> partitions;

    private DynamicPartitionLoader(Table table, Duration refreshInterval, int maxPartitionNum) {
        this.table = table;
        this.refreshInterval = refreshInterval;
        this.maxPartitionNum = maxPartitionNum;
    }

    public void open() {
        RowType partitionType = table.rowType().project(table.partitionKeys());
        this.comparator = CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());
        this.partitions = Collections.emptyList();
    }

    public void addPartitionKeysTo(List<String> joinKeys, List<String> projectFields) {
        List<String> partitionKeys = table.partitionKeys();
        checkArgument(joinKeys.stream().noneMatch(partitionKeys::contains));
        joinKeys.addAll(partitionKeys);

        partitionKeys.stream().filter(k -> !projectFields.contains(k)).forEach(projectFields::add);
    }

    public List<BinaryRow> partitions() {
        return partitions;
    }

    /** @return true if partition changed. */
    public boolean checkRefresh() {
        if (lastRefresh != null
                && !lastRefresh.plus(refreshInterval).isBefore(LocalDateTime.now())) {
            return false;
        }

        List<BinaryRow> newPartitions = getMaxPartitions();
        lastRefresh = LocalDateTime.now();

        if (newPartitions.size() != partitions.size()) {
            partitions = newPartitions;
            return true;
        } else {
            for (int i = 0; i < newPartitions.size(); i++) {
                if (comparator.compare(newPartitions.get(i), partitions.get(i)) != 0) {
                    partitions = newPartitions;
                    return true;
                }
            }
            return false;
        }
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

    @Nullable
    public static DynamicPartitionLoader of(Table table) {
        Options options = Options.fromMap(table.options());
        String dynamicPartition = options.get(LOOKUP_DYNAMIC_PARTITION);
        if (dynamicPartition == null) {
            return null;
        }

        int maxPartitionNum;
        switch (dynamicPartition.toLowerCase()) {
            case MAX_PT:
                maxPartitionNum = 1;
                break;
            case MAX_TWO_PT:
                maxPartitionNum = 2;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported dynamic partition pattern: " + dynamicPartition);
        }

        Duration refresh =
                options.get(FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL);
        return new DynamicPartitionLoader(table, refresh, maxPartitionNum);
    }
}

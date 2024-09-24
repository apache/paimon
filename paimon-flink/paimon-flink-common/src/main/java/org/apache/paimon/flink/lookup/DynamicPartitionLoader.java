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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Dynamic partition for lookup. */
public class DynamicPartitionLoader implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String MAX_PT = "max_pt()";

    private final Table table;
    private final Duration refreshInterval;

    private Comparator<InternalRow> comparator;

    private LocalDateTime lastRefresh;
    @Nullable private BinaryRow partition;

    private DynamicPartitionLoader(Table table, Duration refreshInterval) {
        this.table = table;
        this.refreshInterval = refreshInterval;
    }

    public void open() {
        RowType partitionType = table.rowType().project(table.partitionKeys());
        this.comparator = CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());
    }

    public void addPartitionKeysTo(List<String> joinKeys, List<String> projectFields) {
        List<String> partitionKeys = table.partitionKeys();
        checkArgument(joinKeys.stream().noneMatch(partitionKeys::contains));
        joinKeys.addAll(partitionKeys);

        partitionKeys.stream().filter(k -> !projectFields.contains(k)).forEach(projectFields::add);
    }

    @Nullable
    public BinaryRow partition() {
        return partition;
    }

    /** @return true if partition changed. */
    public boolean checkRefresh() {
        if (lastRefresh != null
                && !lastRefresh.plus(refreshInterval).isBefore(LocalDateTime.now())) {
            return false;
        }

        BinaryRow previous = this.partition;
        partition =
                table.newReadBuilder().newScan().listPartitions().stream()
                        .max(comparator)
                        .orElse(null);
        lastRefresh = LocalDateTime.now();

        return !Objects.equals(previous, partition);
    }

    @Nullable
    public static DynamicPartitionLoader of(Table table) {
        Options options = Options.fromMap(table.options());
        String dynamicPartition = options.get(LOOKUP_DYNAMIC_PARTITION);
        if (dynamicPartition == null) {
            return null;
        }

        if (!dynamicPartition.equalsIgnoreCase(MAX_PT)) {
            throw new UnsupportedOperationException(
                    "Unsupported dynamic partition pattern: " + dynamicPartition);
        }

        Duration refresh =
                options.get(FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL);
        return new DynamicPartitionLoader(table, refresh);
    }
}

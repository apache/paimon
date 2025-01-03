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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Dynamic partition for lookup. */
public class DynamicPartitionLoader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionLoader.class);

    private static final long serialVersionUID = 1L;

    private static final String MAX_PT = "max_pt()";

    private static final String MAX_TWO_PT = "max_two_pt()";

    private final Table table;
    private final Duration refreshInterval;
    private final int maxPartitionNum;
    private final RowDataToObjectArrayConverter partitionConverter;

    private Comparator<InternalRow> comparator;

    private LocalDateTime lastRefresh;
    private List<BinaryRow> partitions;

    private DynamicPartitionLoader(Table table, Duration refreshInterval, int maxPartitionNum) {
        this.table = table;
        this.refreshInterval = refreshInterval;
        this.maxPartitionNum = maxPartitionNum;
        this.partitionConverter =
                new RowDataToObjectArrayConverter(table.rowType().project(table.partitionKeys()));
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

    public Predicate createSpecificPartFilter() {
        Predicate partFilter = null;
        for (BinaryRow partition : partitions) {
            if (partFilter == null) {
                partFilter = createSinglePartFilter(partition);
            } else {
                partFilter = PredicateBuilder.or(partFilter, createSinglePartFilter(partition));
            }
        }
        return partFilter;
    }

    private Predicate createSinglePartFilter(BinaryRow partition) {
        RowType rowType = table.rowType();
        List<String> partitionKeys = table.partitionKeys();
        Object[] partitionSpec = partitionConverter.convert(partition);
        Map<String, Object> partitionMap = new HashMap<>(partitionSpec.length);
        for (int i = 0; i < partitionSpec.length; i++) {
            partitionMap.put(partitionKeys.get(i), partitionSpec[i]);
        }

        // create partition predicate base on rowType instead of partitionType
        return createPartitionPredicate(rowType, partitionMap);
    }

    /** @return true if partition changed. */
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

    @Nullable
    public static DynamicPartitionLoader of(Table table) {
        Options options = Options.fromMap(table.options());
        String dynamicPartition = options.get(LOOKUP_DYNAMIC_PARTITION);
        if (dynamicPartition == null) {
            return null;
        }

        checkArgument(
                !table.partitionKeys().isEmpty(),
                "{} is not supported for non-partitioned table.",
                LOOKUP_DYNAMIC_PARTITION);

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

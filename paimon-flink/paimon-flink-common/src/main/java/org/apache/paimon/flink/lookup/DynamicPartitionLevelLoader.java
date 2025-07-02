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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Dynamic partition loader which can specify the partition level to load for lookup. */
public class DynamicPartitionLevelLoader extends DynamicPartitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionLevelLoader.class);

    private static final long serialVersionUID = 2L;

    private final int maxPartitionLoadLevel;
    private final List<InternalRow.FieldGetter> fieldGetters;

    DynamicPartitionLevelLoader(
            FileStoreTable table,
            Duration refreshInterval,
            Map<String, String> partitionLoadConfig) {
        super(table, refreshInterval);
        maxPartitionLoadLevel =
                getMaxPartitionLoadLevel(partitionLoadConfig, table.partitionKeys());
        fieldGetters = createPartitionFieldGetters();
    }

    @Override
    public boolean checkRefresh() {
        if (lastRefresh != null
                && !lastRefresh.plus(refreshInterval).isBefore(LocalDateTime.now())) {
            return false;
        }

        LOG.info(
                "DynamicPartitionLevelLoader(maxPartitionLoadLevel={},table={}) refreshed after {} second(s), refreshing",
                maxPartitionLoadLevel,
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
                    "DynamicPartitionLevelLoader(maxPartitionLoadLevel={},table={}) didn't find new partitions.",
                    maxPartitionLoadLevel,
                    table.name());
            return false;
        }
    }

    @Override
    protected List<BinaryRow> getMaxPartitions() {
        List<BinaryRow> newPartitions =
                table.newReadBuilder().newScan().listPartitions().stream()
                        .sorted(comparator.reversed())
                        .collect(Collectors.toList());

        if (maxPartitionLoadLevel == table.partitionKeys().size() - 1) {
            // if maxPartitionLoadLevel is the max partition level, we only need to load the max
            // partition
            if (newPartitions.size() <= 1) {
                return newPartitions;
            } else {
                return newPartitions.subList(0, 1);
            }
        }

        for (int level = 0; level <= maxPartitionLoadLevel; level++) {
            newPartitions = extractMaxPartitionsForFixedLevel(newPartitions, level);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "DynamicPartitionLevelLoader(currentPartitionLoadLevel={},table={}) finds new partitions: {}.",
                        level,
                        table.name(),
                        partitionsToString(newPartitions));
            }
        }
        return newPartitions;
    }

    private int getMaxPartitionLoadLevel(
            Map<String, String> partitionLoadConfig, List<String> partitionFields) {
        Preconditions.checkArgument(partitionLoadConfig.size() <= partitionFields.size());

        int maxLoadLevel = -1;
        for (int i = 0; i < partitionFields.size(); i++) {
            String config = partitionLoadConfig.getOrDefault(partitionFields.get(i), null);

            if (i == 0) {
                Preconditions.checkArgument(
                        config != null, "the top level partition must set load config.");
            }
            if (config != null) {
                Preconditions.checkArgument(
                        config.equals(MAX_PT),
                        "unsupported load config '{}' for partition field '{}', currently only support '{}'.",
                        config,
                        partitionFields.get(i),
                        MAX_PT);
                maxLoadLevel = Math.max(maxLoadLevel, i);

            } else {
                // if level-i partition don't set config, we won't consider the partition level
                // which is greater than level-i
                return maxLoadLevel;
            }
        }
        return maxLoadLevel;
    }

    private List<InternalRow.FieldGetter> createPartitionFieldGetters() {
        List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

        RowType partitionType = table.rowType().project(table.partitionKeys());

        for (int i = 0; i < maxPartitionLoadLevel + 1; i++) {
            fieldGetters.add(InternalRow.createFieldGetter(partitionType.getTypeAt(i), i));
        }
        return fieldGetters;
    }

    private List<BinaryRow> extractMaxPartitionsForFixedLevel(
            List<BinaryRow> partitions, int level) {
        AtomicInteger currentDistinct = new AtomicInteger(0);
        Object lastField = null;
        for (int i = 0; i < partitions.size(); i++) {
            BinaryRow partition = partitions.get(i);
            Object newField = fieldGetters.get(level).getFieldOrNull(partition);
            // if newField is null, it's the default partition
            if (newField == null) {
                newField = "__DEFAULT_PARTITION__";
            }
            if (lastField == null || !lastField.equals(newField)) {
                lastField = newField;
                if (currentDistinct.addAndGet(1) > 1) {
                    return partitions.subList(0, i);
                }
            }
        }
        return partitions;
    }

    private void logNewPartitions() {
        String partitionsStr = partitionsToString(partitions);

        LOG.info(
                "DynamicPartitionLevelLoader(maxPartitionLoadLevel={},table={}) finds new partitions: {}.",
                maxPartitionLoadLevel,
                table.name(),
                partitionsStr);
    }
}

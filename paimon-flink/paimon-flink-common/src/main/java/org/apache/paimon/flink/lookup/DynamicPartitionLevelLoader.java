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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Dynamic partition loader which can specify the partition level to load for lookup. */
public class DynamicPartitionLevelLoader extends DynamicPartitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionLevelLoader.class);

    private static final long serialVersionUID = 1L;

    private final int maxPartitionLoadLevel;
    private final List<InternalRow.FieldGetter> fieldGetters;

    private final String defaultPartitionName;

    DynamicPartitionLevelLoader(
            FileStoreTable table,
            Duration refreshInterval,
            Map<String, String> partitionLoadConfig) {
        super(table, refreshInterval);
        maxPartitionLoadLevel =
                getMaxPartitionLoadLevel(partitionLoadConfig, table.partitionKeys());
        fieldGetters = createPartitionFieldGetters();
        defaultPartitionName = table.coreOptions().partitionDefaultName();

        LOG.info(
                "Init DynamicPartitionLevelLoader(table={}),maxPartitionLoadLevel is {}",
                table.name(),
                maxPartitionLoadLevel);
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
                // if level-i partition don't set config, partition level which is greater than
                // level-i should not set config
                for (int j = i + 1; j < partitionFields.size(); j++) {
                    Preconditions.checkArgument(
                            !partitionLoadConfig.containsKey(partitionFields.get(j)),
                            "partition field(level=%s,name=%s) don't set config, "
                                    + "but the sub partition field(level=%s,name=%s) set config, this is unsupported.",
                            i,
                            partitionFields.get(i),
                            j,
                            partitionFields.get(j));
                }

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
        int currentDistinct = 0;
        Object lastField = null;
        for (int i = 0; i < partitions.size(); i++) {
            BinaryRow partition = partitions.get(i);
            Object newField = fieldGetters.get(level).getFieldOrNull(partition);
            // if newField is null, it's the default partition
            if (newField == null) {
                newField = defaultPartitionName;
            }
            if (!newField.equals(lastField)) {
                lastField = newField;
                if (++currentDistinct > 1) {
                    return partitions.subList(0, i);
                }
            }
        }
        return partitions;
    }
}

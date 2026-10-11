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
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Dynamic partition loader which can specify the partition level to load for lookup. */
public class DynamicPartitionLevelLoader extends DynamicPartitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionLevelLoader.class);

    private static final long serialVersionUID = 1L;

    private final int maxPartitionLoadLevel;

    DynamicPartitionLevelLoader(
            Table table, Duration refreshInterval, Map<String, String> partitionLoadConfig) {
        super(table, refreshInterval);
        maxPartitionLoadLevel =
                getMaxPartitionLoadLevel(partitionLoadConfig, table.partitionKeys());

        LOG.info(
                "Init DynamicPartitionLevelLoader(table={}),maxPartitionLoadLevel is {}",
                table.name(),
                maxPartitionLoadLevel);
    }

    @Override
    public List<BinaryRow> getMaxPartitions() {
        List<BinaryRow> newPartitions =
                table.newReadBuilder().newScan().topNPartitions(1, maxPartitionLoadLevel + 1);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "DynamicPartitionLevelLoader(currentPartitionLoadLevel={},table={}) finds new partitions: {}.",
                    maxPartitionLoadLevel,
                    table.name(),
                    partitionsToString(newPartitions));
        }

        return newPartitions;
    }

    private int getMaxPartitionLoadLevel(Map<String, String> toLoad, List<String> fields) {
        Preconditions.checkArgument(toLoad.size() <= fields.size());
        int maxLoadLevel = fields.size() - 1;
        for (int i = 0; i < fields.size(); i++) {
            if (!toLoad.containsKey(fields.get(i))) {
                maxLoadLevel = i - 1;
                break;
            }
        }
        Preconditions.checkArgument(
                maxLoadLevel >= 0, "the top level partition must set load config.");
        for (int i = maxLoadLevel + 1; i < fields.size(); i++) {
            Preconditions.checkArgument(
                    !toLoad.containsKey(fields.get(i)),
                    "Max load level is %s, "
                            + "but partition field %s with a higher level %s sets MAX_PT.",
                    maxLoadLevel,
                    fields.get(i),
                    i);
        }
        return maxLoadLevel;
    }
}

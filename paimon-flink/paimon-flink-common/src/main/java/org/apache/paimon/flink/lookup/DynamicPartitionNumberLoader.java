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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/** Dynamic partition loader which can specify the max partition number to load for lookup. */
public class DynamicPartitionNumberLoader extends DynamicPartitionLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionNumberLoader.class);

    private static final long serialVersionUID = 1L;

    private final int maxPartitionNum;

    DynamicPartitionNumberLoader(Table table, Duration refreshInterval, int maxPartitionNum) {
        super(table, refreshInterval);
        this.maxPartitionNum = maxPartitionNum;
        LOG.info(
                "Init DynamicPartitionNumberLoader(table={}),maxPartitionNum is {}",
                table.name(),
                maxPartitionNum);
    }

    @Override
    public List<BinaryRow> getMaxPartitions() {
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

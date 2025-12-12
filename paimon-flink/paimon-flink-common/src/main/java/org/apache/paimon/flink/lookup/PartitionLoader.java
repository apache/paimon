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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Specify partitions for lookup tables. */
public abstract class PartitionLoader implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String MAX_PT = "max_pt()";
    private static final String MAX_TWO_PT = "max_two_pt()";

    protected final Table table;
    private final RowDataToObjectArrayConverter partitionConverter;

    protected transient List<BinaryRow> partitions;

    protected PartitionLoader(Table table) {
        this.table = table;
        this.partitionConverter =
                new RowDataToObjectArrayConverter(table.rowType().project(table.partitionKeys()));
    }

    public void open() {
        this.partitions = new ArrayList<>();
    }

    public List<BinaryRow> partitions() {
        return partitions;
    }

    public void addPartitionKeysTo(List<String> joinKeys, List<String> projectFields) {
        List<String> partitionKeys = table.partitionKeys();
        Preconditions.checkArgument(
                joinKeys.stream().noneMatch(partitionKeys::contains),
                "Currently, Paimon lookup table with partitions does not support partition keys in join keys.");
        joinKeys.addAll(partitionKeys);

        partitionKeys.stream().filter(k -> !projectFields.contains(k)).forEach(projectFields::add);
    }

    public Predicate createSpecificPartFilter() {
        // create partition predicate base on rowType instead of partitionType
        return PartitionPredicate.createPartitionPredicate(
                table.rowType(), partitionConverter, partitions);
    }

    /** @return true if partition changed. */
    public abstract boolean checkRefresh();

    @Nullable
    public static PartitionLoader of(Table table) {
        Options options = Options.fromMap(table.options());
        String scanPartitions = options.get(FlinkConnectorOptions.SCAN_PARTITIONS);
        if (scanPartitions == null) {
            return null;
        }

        Preconditions.checkArgument(
                !table.partitionKeys().isEmpty(),
                "{} is not supported for non-partitioned table.",
                FlinkConnectorOptions.SCAN_PARTITIONS.key());

        int maxPartitionNum = -1;
        switch (scanPartitions.toLowerCase()) {
            case MAX_PT:
                maxPartitionNum = 1;
                break;
            case MAX_TWO_PT:
                maxPartitionNum = 2;
                break;
        }

        Duration refresh =
                options.get(FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL);

        if (maxPartitionNum == -1) {
            if (scanPartitions.contains(MAX_PT)) {
                return new DynamicPartitionLevelLoader(
                        table,
                        refresh,
                        ParameterUtils.parseCommaSeparatedKeyValues(scanPartitions));
            }

            return new StaticPartitionLoader(
                    table, ParameterUtils.getPartitions(scanPartitions.split(";")));
        } else {
            return new DynamicPartitionNumberLoader(table, refresh, maxPartitionNum);
        }
    }
}

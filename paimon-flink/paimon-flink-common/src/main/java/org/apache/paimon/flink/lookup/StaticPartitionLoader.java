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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;

/** {@link PartitionLoader} for specified static partitions. */
public class StaticPartitionLoader extends PartitionLoader {

    private final List<Map<String, String>> scanPartitions;

    protected StaticPartitionLoader(Table table, List<Map<String, String>> scanPartitions) {
        super(table);
        this.scanPartitions = scanPartitions;
    }

    @Override
    public void open() {
        partitions = new ArrayList<>();
        RowType partitionType = table.rowType().project(table.partitionKeys());
        String defaultPartitionName = Options.fromMap(table.options()).get(PARTITION_DEFAULT_NAME);
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        for (Map<String, String> spec : scanPartitions) {
            GenericRow row =
                    InternalRowPartitionComputer.convertSpecToInternalRow(
                            spec, partitionType, defaultPartitionName);
            partitions.add(serializer.toBinaryRow(row).copy());
        }
    }

    @Override
    public boolean checkRefresh() {
        return false;
    }
}

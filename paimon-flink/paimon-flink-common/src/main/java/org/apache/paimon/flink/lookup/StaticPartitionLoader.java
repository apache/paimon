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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.List;
import java.util.Map;

/** {@link PartitionLoader} for specified static partitions. */
public class StaticPartitionLoader extends PartitionLoader {

    private final List<Map<String, String>> scanPartitions;

    protected StaticPartitionLoader(
            FileStoreTable table, List<Map<String, String>> scanPartitions) {
        super(table);
        this.scanPartitions = scanPartitions;
    }

    @Override
    public boolean checkRefresh() {
        if (partitions.isEmpty()) {
            RowType partitionType = table.schema().logicalPartitionType();
            InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
            for (Map<String, String> spec : scanPartitions) {
                GenericRow row =
                        InternalRowPartitionComputer.convertSpecToInternalRow(
                                spec, partitionType, table.coreOptions().partitionDefaultName());
                partitions.add(serializer.toBinaryRow(row).copy());
            }
            return true;
        } else {
            return false;
        }
    }
}

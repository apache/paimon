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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.table.connector.source.DynamicFilteringData;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Pre-calculate which splits each task should process by given {@link DynamicFilteringData}, and
 * then distribute the splits fairly.
 */
public class DynamicPartitionPruningPreAssignSplitAssigner extends PreAssignSplitAssigner {
    public DynamicPartitionPruningPreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        super(
                splitBatchSize,
                parallelism,
                splits.stream()
                        .filter(s -> filter(partitionRowProjection, dynamicFilteringData, s))
                        .collect(Collectors.toList()));
    }

    private static boolean filter(
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData,
            FileStoreSourceSplit sourceSplit) {
        DataSplit dataSplit = (DataSplit) sourceSplit.split();
        BinaryRow partition = dataSplit.partition();
        FlinkRowData projected = new FlinkRowData(partitionRowProjection.apply(partition));
        return dynamicFilteringData.contains(projected);
    }
}

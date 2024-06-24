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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.PartitionPathUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.partition.PartitionMarkDone.closeActions;
import static org.apache.paimon.flink.sink.partition.PartitionMarkDone.getPartitionMarkDoneActions;
import static org.apache.paimon.flink.sink.partition.PartitionMarkDone.markDone;

/** Table partition mark done action for Flink. */
public class PartitionMarkDoneAction extends TableActionBase {

    private final FileStoreTable fileStoreTable;
    private final List<Map<String, String>> partitions;
    private final CoreOptions coreOptions;
    private final Options options;

    public PartitionMarkDoneAction(
            String warehouse,
            String databaseName,
            String tableName,
            List<Map<String, String>> partitions,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports mark_partition_done action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        this.fileStoreTable = (FileStoreTable) table;
        this.partitions = partitions;
        this.coreOptions = fileStoreTable.coreOptions();
        this.options = coreOptions.toConfiguration();
    }

    @Override
    public void run() throws Exception {
        List<org.apache.paimon.flink.sink.partition.PartitionMarkDoneAction> actions =
                getPartitionMarkDoneActions(fileStoreTable, coreOptions, options);

        List<String> partitionPaths = getPartitionPaths(fileStoreTable, partitions);

        markDone(partitionPaths, actions);

        closeActions(actions);
    }

    public static List<String> getPartitionPaths(
            FileStoreTable fileStoreTable, List<Map<String, String>> partitions) {
        return partitions.stream()
                .map(
                        partition ->
                                PartitionPathUtils.generatePartitionPath(
                                        partition, fileStoreTable.store().partitionType()))
                .collect(Collectors.toList());
    }
}

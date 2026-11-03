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
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.PartitionPathUtils;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.sink.listener.PartitionMarkDoneListener.markDone;

/** Table partition mark done action for Flink. */
public class MarkPartitionDoneAction extends TableActionBase implements LocalAction {

    private final List<Map<String, String>> partitions;

    public MarkPartitionDoneAction(
            String databaseName,
            String tableName,
            List<Map<String, String>> partitions,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable) && !(table instanceof FormatTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable and FormatTable support mark_partition_done "
                                    + "action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        this.partitions = partitions;
    }

    @Override
    public void executeLocally() throws Exception {
        if (table instanceof FormatTable) {
            FormatTable formatTable = (FormatTable) table;
            List<PartitionMarkDoneAction> actions =
                    PartitionMarkDoneAction.createActions(
                            getClass().getClassLoader(),
                            formatTable,
                            CoreOptions.fromMap(formatTable.options()));
            List<String> partitionPaths =
                    PartitionPathUtils.generatePartitionPaths(
                            partitions, formatTable.partitionType());
            try {
                for (PartitionMarkDoneAction action : actions) {
                    action.markDone(partitionPaths);
                }
            } finally {
                IOUtils.closeAllQuietly(actions);
            }
            return;
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<PartitionMarkDoneAction> actions =
                PartitionMarkDoneAction.createActions(
                        getClass().getClassLoader(), fileStoreTable, fileStoreTable.coreOptions());
        try {
            markDone(
                    PartitionPathUtils.generatePartitionPaths(
                            partitions, fileStoreTable.store().partitionType()),
                    actions);
        } finally {
            IOUtils.closeAllQuietly(actions);
        }
    }
}

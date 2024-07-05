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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.table.FileStoreTable;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Action to mark partitions done. */
public interface PartitionMarkDoneAction extends Closeable {

    void markDone(String partition) throws Exception;

    static List<PartitionMarkDoneAction> createActions(
            FileStoreTable fileStoreTable, CoreOptions options) {
        return Arrays.asList(options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).split(","))
                .stream()
                .map(
                        action -> {
                            switch (action) {
                                case "success-file":
                                    return new SuccessFileMarkDoneAction(
                                            fileStoreTable.fileIO(), fileStoreTable.location());
                                case "done-partition":
                                    return new AddDonePartitionAction(
                                            createMetastoreClient(fileStoreTable, options));
                                case "mark-event":
                                    return new MarkPartitionDoneEventAction(
                                            createMetastoreClient(fileStoreTable, options));
                                default:
                                    throw new UnsupportedOperationException(action);
                            }
                        })
                .collect(Collectors.toList());
    }

    static MetastoreClient createMetastoreClient(FileStoreTable table, CoreOptions options) {
        MetastoreClient.Factory metastoreClientFactory =
                table.catalogEnvironment().metastoreClientFactory();

        if (options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).contains("done-partition")) {
            checkNotNull(
                    metastoreClientFactory,
                    "Cannot mark done partition for table without metastore.");
            checkArgument(
                    options.partitionedTableInMetastore(),
                    "Table should enable %s",
                    METASTORE_PARTITIONED_TABLE.key());
        }

        return metastoreClientFactory.create();
    }
}

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

package org.apache.paimon.partition.actions;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.utils.StringUtils;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_CUSTOM_CLASS;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Action to mark partitions done. */
public interface PartitionMarkDoneAction extends Closeable {

    String SUCCESS_FILE = "success-file";
    String DONE_PARTITION = "done-partition";
    String MARK_EVENT = "mark-event";
    String CUSTOM = "custom";

    void markDone(String partition) throws Exception;

    static List<PartitionMarkDoneAction> createActions(
            ClassLoader cl, FileStoreTable fileStoreTable, CoreOptions options) {
        return Arrays.stream(options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).split(","))
                .map(
                        action -> {
                            switch (action.toLowerCase()) {
                                case SUCCESS_FILE:
                                    return new SuccessFileMarkDoneAction(
                                            fileStoreTable.fileIO(), fileStoreTable.location());
                                case DONE_PARTITION:
                                    return new AddDonePartitionAction(
                                            createPartitionHandler(fileStoreTable, options));
                                case MARK_EVENT:
                                    return new MarkPartitionDoneEventAction(
                                            createPartitionHandler(fileStoreTable, options));
                                case CUSTOM:
                                    return generateCustomMarkDoneAction(cl, options);
                                default:
                                    throw new UnsupportedOperationException(action);
                            }
                        })
                .collect(Collectors.toList());
    }

    static PartitionMarkDoneAction generateCustomMarkDoneAction(
            ClassLoader cl, CoreOptions options) {
        if (StringUtils.isNullOrWhitespaceOnly(options.partitionMarkDoneCustomClass())) {
            throw new IllegalArgumentException(
                    String.format(
                            "You need to set [%s] when you add [%s] mark done action in your property [%s].",
                            PARTITION_MARK_DONE_CUSTOM_CLASS.key(),
                            CUSTOM,
                            PARTITION_MARK_DONE_ACTION.key()));
        }
        String customClass = options.partitionMarkDoneCustomClass();
        try {
            return (PartitionMarkDoneAction) cl.loadClass(customClass).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(
                    "Can not create new instance for custom class from " + customClass, e);
        }
    }

    static PartitionHandler createPartitionHandler(FileStoreTable table, CoreOptions options) {
        PartitionHandler partitionHandler = table.catalogEnvironment().partitionHandler();

        if (options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).contains("done-partition")) {
            checkNotNull(
                    partitionHandler, "Cannot mark done partition for table without metastore.");
            checkArgument(
                    options.partitionedTableInMetastore(),
                    "Table should enable %s",
                    METASTORE_PARTITIONED_TABLE.key());
        }

        return partitionHandler;
    }
}

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
import org.apache.paimon.table.PartitionMarkDone;
import org.apache.paimon.table.PartitionModification;
import org.apache.paimon.utils.StringUtils;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_CUSTOM_CLASS;
import static org.apache.paimon.CoreOptions.PartitionMarkDoneAction.CUSTOM;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Action to mark partitions done. */
public interface PartitionMarkDoneAction extends Closeable {

    default void open(FileStoreTable fileStoreTable, CoreOptions options) {}

    void markDone(String partition) throws Exception;

    static List<PartitionMarkDoneAction> createActions(
            ClassLoader cl, FileStoreTable fileStoreTable, CoreOptions options) {
        return options.partitionMarkDoneActions().stream()
                .map(
                        action -> {
                            PartitionMarkDoneAction instance;
                            switch (action) {
                                case SUCCESS_FILE:
                                    instance =
                                            new SuccessFileMarkDoneAction(
                                                    fileStoreTable.fileIO(),
                                                    fileStoreTable.location());
                                    break;
                                case DONE_PARTITION:
                                    instance =
                                            new AddDonePartitionAction(
                                                    createPartitionModification(
                                                            fileStoreTable, options));
                                    break;
                                case MARK_EVENT:
                                    instance =
                                            new MarkPartitionDoneEventAction(
                                                    createPartitionMarkDone(
                                                            fileStoreTable, options));
                                    break;
                                case HTTP_REPORT:
                                    instance = new HttpReportMarkDoneAction();
                                    break;
                                case CUSTOM:
                                    instance = generateCustomMarkDoneAction(cl, options);
                                    break;
                                default:
                                    throw new UnsupportedOperationException(action.toString());
                            }
                            instance.open(fileStoreTable, options);
                            return instance;
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

    static PartitionModification createPartitionModification(
            FileStoreTable table, CoreOptions options) {
        PartitionModification partitionModification =
                table.catalogEnvironment().partitionModification();

        if (options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).contains("done-partition")) {
            checkNotNull(
                    partitionModification,
                    "Cannot mark done partition for table without metastore.");
            checkArgument(
                    options.partitionedTableInMetastore(),
                    "Table should enable %s",
                    METASTORE_PARTITIONED_TABLE.key());
        }

        return partitionModification;
    }

    static PartitionMarkDone createPartitionMarkDone(FileStoreTable table, CoreOptions options) {
        PartitionMarkDone partitionMarkDone = table.catalogEnvironment().partitionMarkDone();

        if (options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).contains("mark-event")) {
            checkNotNull(
                    partitionMarkDone, "Cannot mark done partition for table without metastore.");
            checkArgument(
                    options.partitionedTableInMetastore(),
                    "Table should enable %s",
                    METASTORE_PARTITIONED_TABLE.key());
        }

        return partitionMarkDone;
    }
}

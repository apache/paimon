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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.dataevolution.DataEvolutionDeleteSink;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;

/** Internal implementation which logically deletes rows from a Data Evolution append table. */
class DataEvolutionDelete implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionDelete.class);

    private final DeleteAction action;
    private final String filter;
    private final long baseSnapshotId;

    private int sinkParallelism = 1;

    DataEvolutionDelete(DeleteAction action, String filter) {
        this.action = action;
        Preconditions.checkArgument(
                filter != null && !filter.trim().isEmpty(),
                "Deletion filter must not be null or blank.");
        this.filter = filter;

        if (!(action.table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports Data Evolution delete. The table type is '%s'.",
                            action.table.getClass().getName()));
        }

        FileStoreTable storeTable = (FileStoreTable) action.table;
        DataEvolutionDeleteSink.validateTable(storeTable);
        Long latestSnapshotId = storeTable.snapshotManager().latestSnapshotId();
        if (latestSnapshotId == null) {
            throw new UnsupportedOperationException(
                    "Data-evolution delete action doesn't support deleting from an empty table.");
        }
        this.baseSnapshotId = latestSnapshotId;
    }

    DataEvolutionDelete withSinkParallelism(int sinkParallelism) {
        Preconditions.checkArgument(
                sinkParallelism > 0,
                "Sink parallelism must be a positive integer, but is %s.",
                sinkParallelism);
        this.sinkParallelism = sinkParallelism;
        return this;
    }

    /** Builds and executes the Flink batch topology. */
    TableResult runInternal() {
        FileStoreTable storeTable = (FileStoreTable) action.table;
        String query =
                String.format(
                        "SELECT `_ROW_ID` FROM `%s`.`%s`.`%s$row_tracking` "
                                + "/*+ OPTIONS('scan.snapshot-id'='%d', '%s'='full') */ WHERE %s",
                        action.catalogName,
                        action.identifier.getDatabaseName(),
                        action.identifier.getObjectName(),
                        baseSnapshotId,
                        CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(),
                        filter);
        LOG.info("Data-evolution delete source query: {}", query);

        Table matchedRows = action.batchTEnv.sqlQuery(query);
        DataStream<Long> rowIds =
                action.batchTEnv
                        .toDataStream(matchedRows)
                        .map(
                                (MapFunction<Row, Long>) row -> (Long) row.getField(0),
                                TypeInformation.of(Long.class));

        DataStreamSink<?> end =
                new DataEvolutionDeleteSink(storeTable, baseSnapshotId, sinkParallelism)
                        .sinkFrom(rowIds);
        return action.executeInternal(
                Collections.singletonList(end.getTransformation()),
                Collections.singletonList(action.identifier.getFullName()));
    }

    @VisibleForTesting
    static String rewriteGroup(
            String bucketPath,
            @Nullable String oldIndexFile,
            String anchorFilePath,
            int parallelism) {
        return DataEvolutionDeleteSink.rewriteGroup(
                bucketPath, oldIndexFile, anchorFilePath, parallelism);
    }
}

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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;

/** Delete from table action for Flink, dispatching by table type. */
public class DeleteAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteAction.class);

    private final String filter;
    @Nullable private final DataEvolutionDelete dataEvolutionDelete;
    @Nullable private String[] sourceSqls;

    public DeleteAction(
            String databaseName,
            String tableName,
            String filter,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        Preconditions.checkArgument(
                filter != null && !filter.trim().isEmpty(),
                "Deletion filter must not be null or blank.");
        this.filter = filter;

        if (table instanceof FileStoreTable
                && ((FileStoreTable) table).coreOptions().dataEvolutionEnabled()) {
            dataEvolutionDelete = new DataEvolutionDelete(this, filter);
        } else {
            dataEvolutionDelete = null;
        }
    }

    /** SQL statements used to configure the batch environment and register external sources. */
    public DeleteAction withSourceSqls(String... sourceSqls) {
        this.sourceSqls = sourceSqls;
        return this;
    }

    /** Configures deletion-vector aggregation and writing parallelism for Data Evolution tables. */
    public DeleteAction withSinkParallelism(int sinkParallelism) {
        if (dataEvolutionDelete == null) {
            throw new UnsupportedOperationException(
                    "Sink parallelism is only supported when deleting from a Data Evolution table.");
        }
        dataEvolutionDelete.withSinkParallelism(sinkParallelism);
        return this;
    }

    @Override
    public void run() throws Exception {
        handleSqls();
        if (dataEvolutionDelete != null) {
            dataEvolutionDelete.runInternal().await();
            return;
        }

        runPrimaryKeyDelete();
    }

    private void runPrimaryKeyDelete() throws Exception {
        if (!(table instanceof FileStoreTable)
                || ((FileStoreTable) table).schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Delete does not support regular append-only tables. "
                            + "Only primary-key tables and Data Evolution append tables are supported.");
        }

        CoreOptions.MergeEngine mergeEngine = CoreOptions.fromMap(table.options()).mergeEngine();
        if (mergeEngine != DEDUPLICATE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Delete is executed in batch mode, but merge engine %s can not support batch delete.",
                            mergeEngine));
        }

        LOG.debug("Run delete action with filter '{}'.", filter);

        Table queriedTable =
                batchTEnv.sqlQuery(
                        String.format(
                                "SELECT * FROM `%s`.`%s`.`%s` WHERE %s",
                                catalogName,
                                identifier.getDatabaseName(),
                                identifier.getObjectName(),
                                filter));

        List<DataStructureConverter<Object, Object>> converters =
                queriedTable.getResolvedSchema().getColumnDataTypes().stream()
                        .map(DataStructureConverters::getConverter)
                        .collect(Collectors.toList());

        DataStream<RowData> dataStream =
                batchTEnv
                        .toChangelogStream(queriedTable)
                        .map(
                                row -> {
                                    int arity = row.getArity();
                                    GenericRowData rowData =
                                            new GenericRowData(RowKind.DELETE, arity);
                                    for (int i = 0; i < arity; i++) {
                                        rowData.setField(
                                                i,
                                                converters
                                                        .get(i)
                                                        .toInternalOrNull(row.getField(i)));
                                    }
                                    return rowData;
                                });

        batchSink(dataStream).await();
    }

    private void handleSqls() {
        // NOTE: a source SQL statement may change the current catalog and database. Both target
        // query paths therefore use fully-qualified target identifiers.
        if (sourceSqls != null) {
            for (int i = 0; i < sourceSqls.length; i++) {
                try {
                    batchTEnv.executeSql(sourceSqls[i]).await();
                } catch (Exception e) {
                    // Source DDL may contain JDBC credentials. Do not include the SQL or the
                    // original exception (whose message may echo the SQL) in logs or the wrapper.
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    String message =
                            String.format(
                                    "Failed to execute source SQL statement %s (cause type: %s).",
                                    i + 1, e.getClass().getName());
                    LOG.error(message);
                    throw new RuntimeException(message);
                }
            }
        }
    }
}

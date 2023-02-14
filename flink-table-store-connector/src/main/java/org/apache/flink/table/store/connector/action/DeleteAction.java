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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.store.connector.FlinkCatalog;
import org.apache.flink.table.store.connector.sink.FlinkSinkBuilder;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.SupportsWrite;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.store.connector.action.Action.getTablePath;
import static org.apache.flink.table.store.file.catalog.Catalog.DEFAULT_DATABASE;

/** Delete from table action for Flink. */
public class DeleteAction extends AbstractActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteAction.class);

    private final FlinkCatalog flinkCatalog;

    @Nullable private String filter;

    public DeleteAction(Path tablePath) {
        super(tablePath);
        flinkCatalog = new FlinkCatalog(catalog, "table-store", DEFAULT_DATABASE);
    }

    public DeleteAction withFilter(String filter) {
        this.filter = filter;
        return this;
    }

    public static Optional<Action> create(String[] args) {
        LOG.info("Delete job args: {}", String.join(" ", args));

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        Path tablePath = getTablePath(params);

        if (tablePath == null) {
            return Optional.empty();
        }

        DeleteAction action = new DeleteAction(tablePath);

        if (params.has("where")) {
            String filter = params.get("where");
            if (filter == null) {
                return Optional.empty();
            }

            action.withFilter(filter);
        }

        return Optional.of(action);
    }

    private static void printHelp() {
        System.out.println("Action \"delete\" deletes data from a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  delete --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> [--where <filter_spec>]");
        System.out.println("  delete --path <table-path> [--where <filter_spec>]");
        System.out.println();

        System.out.println(
                "The '--where <filter_spec>' part is equal to the 'WHERE' clause in SQL DELETE statement");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  delete --warehouse hdfs:///path/to/warehouse --database test_db --table test_table");
        System.out.println("  It's equal to 'DELETE FROM test_table' which deletes all records");
        System.out.println();
        System.out.println(
                "  delete --path hdfs:///path/to/warehouse/test_db.db/test_table --where id > (SELECT count(*) FROM employee)");
        System.out.println(
                "  It's equal to 'DELETE FROM test_table WHERE id > (SELECT count(*) FROM employee)");
    }

    @Override
    public void run() throws Exception {
        if (filter == null) {
            LOG.debug("Run delete action with no filter.");
            ((SupportsWrite) table)
                    .deleteWhere(
                            UUID.randomUUID().toString(),
                            new ArrayList<>(),
                            Lock.factory(catalog.lockFactory().orElse(null), identifier));
        } else {
            LOG.debug("Run delete action with filter '{}'.", filter);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv =
                    StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());

            tEnv.registerCatalog(flinkCatalog.getName(), flinkCatalog);
            tEnv.useCatalog(flinkCatalog.getName());

            Table queriedTable =
                    tEnv.sqlQuery(
                            String.format(
                                    "SELECT * FROM %s WHERE %s",
                                    identifier.getEscapedFullName('`'), filter));

            List<DataStructureConverter<Object, Object>> converters =
                    queriedTable.getResolvedSchema().getColumnDataTypes().stream()
                            .map(DataStructureConverters::getConverter)
                            .collect(Collectors.toList());

            DataStream<RowData> dataStream =
                    tEnv.toChangelogStream(queriedTable)
                            .map(
                                    row -> {
                                        BiFunction<Integer, Row, Object> fieldConverter =
                                                (i, r) ->
                                                        converters
                                                                .get(i)
                                                                .toInternalOrNull(r.getField(i));

                                        return GenericRowData.ofKind(
                                                RowKind.DELETE,
                                                IntStream.range(0, row.getArity())
                                                        .mapToObj(i -> fieldConverter.apply(i, row))
                                                        .toArray());
                                    });

            new FlinkSinkBuilder((FileStoreTable) table).withInput(dataStream).build();
            env.execute();
        }
    }
}

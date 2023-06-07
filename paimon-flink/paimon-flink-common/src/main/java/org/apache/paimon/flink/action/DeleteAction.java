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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.Action.getTablePath;
import static org.apache.paimon.flink.action.Action.optionalConfigMap;

/** Delete from table action for Flink. */
public class DeleteAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteAction.class);

    private final String filter;

    public DeleteAction(
            String warehouse,
            String databaseName,
            String tableName,
            String filter,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
        changeIgnoreMergeEngine();
        this.filter = filter;
    }

    public static Optional<Action> create(String[] args) {
        LOG.info("Delete job args: {}", String.join(" ", args));

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        Tuple3<String, String, String> tablePath = getTablePath(params);

        if (tablePath == null) {
            return Optional.empty();
        }

        String filter = params.get("where");
        if (filter == null) {
            return Optional.empty();
        }

        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");

        DeleteAction action =
                new DeleteAction(tablePath.f0, tablePath.f1, tablePath.f2, filter, catalogConfig);

        return Optional.of(action);
    }

    private static void printHelp() {
        System.out.println("Action \"delete\" deletes data from a table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  delete --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> --where <filter_spec>");
        System.out.println("  delete --path <table-path> --where <filter_spec>");
        System.out.println();

        System.out.println(
                "The '--where <filter_spec>' part is equal to the 'WHERE' clause in SQL DELETE statement. If you want delete all records, please use overwrite (see doc).");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  delete --path hdfs:///path/to/warehouse/test_db.db/test_table --where id > (SELECT count(*) FROM employee)");
        System.out.println(
                "  It's equal to 'DELETE FROM test_table WHERE id > (SELECT count(*) FROM employee)");
    }

    @Override
    public void run() throws Exception {
        LOG.debug("Run delete action with filter '{}'.", filter);

        Table queriedTable =
                batchTEnv.sqlQuery(
                        String.format(
                                "SELECT * FROM %s WHERE %s",
                                identifier.getEscapedFullName(), filter));

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

        batchSink(dataStream);
    }
}

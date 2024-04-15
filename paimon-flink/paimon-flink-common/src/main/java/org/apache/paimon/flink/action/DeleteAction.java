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
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;

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
        this.filter = filter;
    }

    @Override
    public void run() throws Exception {
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

        batchSink(dataStream).await();
    }
}

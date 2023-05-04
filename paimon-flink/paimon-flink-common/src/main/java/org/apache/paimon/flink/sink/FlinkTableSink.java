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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.WriteMode;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.ChangelogValueCountFileStoreTable;
import org.apache.paimon.table.ChangelogWithKeyFileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.WRITE_MODE;

/** Table sink to create sink. */
public class FlinkTableSink extends FlinkTableSinkBase implements SupportsRowLevelUpdate {

    private Map<Integer, RowData.FieldGetter> updatedColumns = Collections.emptyMap();

    public FlinkTableSink(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            List<Column> columns) {
        super(tableIdentifier, table, context, logStoreTableFactory, columns);
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
        // Since only UPDATE_AFTER type messages can be received at present,
        // AppendOnlyFileStoreTable and ChangelogValueCountFileStoreTable without primary keys
        // cannot correctly handle old data, so they are marked as unsupported. Similarly, it is not
        // allowed to update the primary key column when updating the column of
        // ChangelogWithKeyFileStoreTable, because the old data cannot be handled correctly.
        if (table instanceof ChangelogWithKeyFileStoreTable) {
            Options options = Options.fromMap(table.options());
            Set<String> primaryKeys = new HashSet<>(table.primaryKeys());
            updatedColumns.forEach(
                    column -> {
                        if (primaryKeys.contains(column.getName())) {
                            String errMsg =
                                    String.format(
                                            "Updates to primary keys are not supported, primaryKeys (%s), updatedColumns (%s)",
                                            primaryKeys,
                                            updatedColumns.stream()
                                                    .map(Column::getName)
                                                    .collect(Collectors.toList()));
                            throw new UnsupportedOperationException(errMsg);
                        }
                    });

            if (options.get(MERGE_ENGINE) == MergeEngine.DEDUPLICATE) {
                return new RowLevelUpdateInfo() {};
            } else if (options.get(MERGE_ENGINE) == MergeEngine.PARTIAL_UPDATE) {
                this.updatedColumns = new LinkedHashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    if (primaryKeys.contains(column.getName()) || updatedColumns.contains(column)) {
                        this.updatedColumns.put(
                                i,
                                RowData.createFieldGetter(
                                        column.getDataType().getLogicalType(), i));
                    }
                }
                // Even with partial-update we still need all columns. Because the topology
                // structure is source -> cal -> constraintEnforcer -> sink, in the
                // constraintEnforcer operator, the constraint check will be performed according to
                // the index, not according to the column. So we can't return only some columns,
                // which will cause problems like ArrayIndexOutOfBoundsException.
                return new RowLevelUpdateInfo() {};
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "Only %s and %s types of %s support the update statement.",
                            MergeEngine.DEDUPLICATE,
                            MergeEngine.PARTIAL_UPDATE,
                            MERGE_ENGINE.key()));
        } else if (table instanceof AppendOnlyFileStoreTable
                || table instanceof ChangelogValueCountFileStoreTable) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only tables whose %s is %s and have primary keys support the update statement.",
                            WRITE_MODE.key(), WriteMode.CHANGE_LOG));
        } else {
            throw new UnsupportedOperationException(
                    "Unknown FileStoreTable subclass " + table.getClass().getName());
        }
    }

    @Override
    protected FlinkSinkBuilder createFlinkSinkBuilder(
            DataStream<RowData> dataStream, LogSinkFunction logSinkFunction, Options conf) {
        return super.createFlinkSinkBuilder(dataStream, logSinkFunction, conf)
                .withUpdatedColumns(updatedColumns);
    }
}

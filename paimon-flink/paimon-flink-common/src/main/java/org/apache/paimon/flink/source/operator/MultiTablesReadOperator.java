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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.system.BucketsMultiTable;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.compactOptions;
import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.shouldCompactionTable;

/** this is a doc. */
public class MultiTablesReadOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<Tuple2<Split, String>, RowData> {

    private static final long serialVersionUID = 1L;

    private final Catalog.Loader catalogLoader;
    private final Pattern includingPattern;
    private final Pattern excludingPattern;
    private final Pattern databasePattern;
    private final boolean isStreaming;

    public MultiTablesReadOperator(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.isStreaming = isStreaming;
    }

    private transient Catalog catalog;
    private transient IOManagerImpl ioManager;
    private transient Map<Identifier, BucketsMultiTable> tablesMap;
    private transient Map<Identifier, TableRead> readsMap;
    private transient StreamRecord<RowData> reuseRecord;
    private transient FlinkRowData reuseRow;

    @Override
    public void open() throws Exception {
        super.open();
        ioManager =
                new IOManagerImpl(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        tablesMap = new HashMap<>();
        readsMap = new HashMap<>();
        catalog = catalogLoader.load();
        initializeTableMap();

        this.reuseRow = new FlinkRowData(null);
        this.reuseRecord = new StreamRecord<>(reuseRow);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Split, String>> record) throws Exception {
        Identifier identifier = Identifier.fromString(record.getValue().f1);
        TableRead read = getTableRead(identifier);
        try (CloseableIterator<InternalRow> iterator =
                read.createReader(record.getValue().f0).toCloseableIterator()) {
            while (iterator.hasNext()) {
                reuseRow.replace(iterator.next());
                output.collect(reuseRecord);
            }
        }
    }

    private void initializeTableMap()
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> databases = catalog.listDatabases();

        for (String databaseName : databases) {
            if (databasePattern.matcher(databaseName).matches()) {
                List<String> tables = catalog.listTables(databaseName);
                for (String tableName : tables) {
                    Identifier identifier = Identifier.create(databaseName, tableName);
                    if (shouldCompactionTable(identifier, includingPattern, excludingPattern)
                            && (!tablesMap.containsKey(identifier))) {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.error(
                                    String.format(
                                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                                            table.getClass().getName()));
                            continue;
                        }

                        BucketsMultiTable bucketsTable =
                                new BucketsMultiTable(
                                                (FileStoreTable) table,
                                                isStreaming,
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName())
                                        .copy(compactOptions(isStreaming));
                        tablesMap.put(identifier, bucketsTable);
                        readsMap.put(
                                identifier,
                                bucketsTable.newReadBuilder().newRead().withIOManager(ioManager));
                    }
                }
            }
        }
    }

    private TableRead getTableRead(Identifier tableId) {
        BucketsMultiTable table = tablesMap.get(tableId);
        if (table == null) {
            try {
                Table newTable = catalog.getTable(tableId);
                if (!(newTable instanceof FileStoreTable)) {
                    LOG.error(
                            String.format(
                                    "Only FileStoreTable supports compact action. The table type is '%s'.",
                                    newTable.getClass().getName()));
                } else {
                    table =
                            new BucketsMultiTable(
                                            (FileStoreTable) newTable,
                                            isStreaming,
                                            tableId.getDatabaseName(),
                                            tableId.getObjectName())
                                    .copy(compactOptions(isStreaming));
                }
                tablesMap.put(tableId, table);
                readsMap.put(tableId, table.newReadBuilder().newRead().withIOManager(ioManager));
            } catch (Catalog.TableNotExistException e) {
                LOG.error(String.format("table: %s not found.", tableId.getFullName()));
            }
        }

        return readsMap.get(tableId);
    }
}

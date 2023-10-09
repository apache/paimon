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

package org.apache.paimon.flink.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.system.FileMonitorTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.tableMatchPattern;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Monitoring new Paimon tables from catalog.
 *   <li>Read incremental files from tables.
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 */
public class FileMonitorSourceFunction extends RichSourceFunction<InternalRow> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileMonitorSourceFunction.class);

    protected final Catalog.Loader catalogLoader;
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;
    protected final long monitorInterval;

    protected transient Catalog catalog;
    protected transient Map<Identifier, StreamTableScan> scanMap;
    protected transient Map<Identifier, TableRead> readMap;
    protected transient SourceContext<InternalRow> ctx;

    protected volatile boolean isRunning = true;

    public FileMonitorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.monitorInterval = monitorInterval;
    }

    public static RowType recordType() {
        RowType.Builder builder = RowType.builder();
        builder.field("_DATABASE", DataTypes.STRING());
        builder.field("_TABLE", DataTypes.STRING());
        for (DataField field : FileMonitorTable.getRowType().getFields()) {
            builder.field(field.name(), field.type());
        }
        return builder.build();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        scanMap = new HashMap<>();
        readMap = new HashMap<>();
        catalog = catalogLoader.load();

        updateTableMap();
    }

    @Override
    public void run(SourceContext<InternalRow> ctx) throws Exception {
        this.ctx = ctx;
        while (isRunning) {
            boolean isEmpty;
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }
                isEmpty = doScan();
            }

            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    private boolean doScan() throws Exception {
        // check for new tables
        updateTableMap();

        List<InternalRow> records = new ArrayList<>();
        for (Map.Entry<Identifier, StreamTableScan> entry : scanMap.entrySet()) {
            Identifier identifier = entry.getKey();
            StreamTableScan scan = entry.getValue();
            TableRead read = readMap.get(identifier);
            GenericRow dbAndTable =
                    GenericRow.of(
                            BinaryString.fromString(identifier.getDatabaseName()),
                            BinaryString.fromString(identifier.getObjectName()));
            read.createReader(scan.plan())
                    .forEachRemaining(row -> records.add(new JoinedRow().replace(dbAndTable, row)));
        }

        records.forEach(ctx::collect);
        return records.isEmpty();
    }

    private void updateTableMap()
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> databases = catalog.listDatabases();
        for (String databaseName : databases) {
            updateTableMap(databaseName);
        }
    }

    private void updateTableMap(String databaseName)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        if (databasePattern.matcher(databaseName).matches()) {
            List<String> tables = catalog.listTables(databaseName);
            for (String tableName : tables) {
                updateTableMap(Identifier.create(databaseName, tableName));
            }
        }
    }

    private void updateTableMap(Identifier identifier) throws Catalog.TableNotExistException {
        if (tableMatchPattern(identifier, includingPattern, excludingPattern)
                && (!scanMap.containsKey(identifier))) {
            Table table = catalog.getTable(identifier);
            if (!(table instanceof FileStoreTable)) {
                LOG.error(
                        String.format(
                                "Only FileStoreTable supports compact action. The table type is '%s'.",
                                table.getClass().getName()));
                return;
            }

            FileStoreTable fileStoreTable = (FileStoreTable) table;
            if (fileStoreTable.bucketMode() != BucketMode.FIXED
                    || fileStoreTable.schema().primaryKeys().isEmpty()) {
                throw new UnsupportedOperationException(
                        "The bucket mode of "
                                + identifier.getFullName()
                                + " is not fixed or the table has no primary key.");
            }

            CoreOptions options = fileStoreTable.coreOptions();
            if (options.mergeEngine() != MergeEngine.DEDUPLICATE) {
                throw new UnsupportedOperationException(
                        "The merge engine of " + identifier.getFullName() + " is DEDUPLICATE.");
            }

            if (options.sequenceField().isPresent()) {
                throw new UnsupportedOperationException(
                        "The sequence field of "
                                + identifier.getFullName()
                                + " is "
                                + options.sequenceField().get()
                                + ".");
            }

            FileMonitorTable monitorTable = new FileMonitorTable(fileStoreTable);
            ReadBuilder readBuilder = monitorTable.newReadBuilder();
            scanMap.put(identifier, readBuilder.newStreamScan());
            readMap.put(identifier, readBuilder.newRead());
        }
    }

    @Override
    public void cancel() {
        // this is to cover the case where cancel() is called before the run()
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            isRunning = false;
        }
    }

    public static DataStream<InternalRow> build(
            StreamExecutionEnvironment env,
            Catalog.Loader catalogLoader,
            Pattern databasePattern,
            Pattern includingPattern,
            Pattern excludingPattern,
            long monitorInterval) {
        FileMonitorSourceFunction function =
                new FileMonitorSourceFunction(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        monitorInterval);
        StreamSource<InternalRow, ?> sourceOperator = new StreamSource<>(function);
        InternalTypeInfo<InternalRow> typeInfo =
                InternalTypeInfo.fromRowType(FileMonitorSourceFunction.recordType());
        return new DataStreamSource<>(
                        env,
                        typeInfo,
                        sourceOperator,
                        false,
                        "FileMonitor",
                        Boundedness.CONTINUOUS_UNBOUNDED)
                .forceNonParallel()
                .partitionCustom(
                        (key, numPartitions) -> key % numPartitions,
                        row ->
                                AddressServerFunction.computeHash(
                                        row.getString(0).toString(),
                                        row.getString(1).toString(),
                                        deserializeBinaryRow(row.getBinary(3)),
                                        row.getInt(4)));
    }
}

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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.system.BucketsTable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.compactOptions;
import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.shouldCompactTable;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Monitoring snapshots of the Paimon table.
 *   <li>Creating the Tuple2<{@link Split}, String> splits corresponding to the incremental files
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link MultiTablesReadOperator} which
 * can have parallelism greater than one.
 *
 * <p>Currently, only dedicated compaction job for multi-tables rely on this monitor.
 */
public abstract class MultiTablesCompactorSourceFunction
        extends RichSourceFunction<Tuple2<Split, String>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTablesCompactorSourceFunction.class);

    protected final Catalog.Loader catalogLoader;
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;
    protected final boolean isStreaming;
    protected final long monitorInterval;

    protected transient Catalog catalog;
    protected transient Map<Identifier, BucketsTable> tablesMap;
    protected transient Map<Identifier, StreamTableScan> scansMap;

    public MultiTablesCompactorSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming,
            long monitorInterval) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.isStreaming = isStreaming;
        this.monitorInterval = monitorInterval;
    }

    protected volatile boolean isRunning = true;

    protected transient SourceContext<Tuple2<Split, String>> ctx;

    @Override
    public void open(Configuration parameters) throws Exception {
        tablesMap = new HashMap<>();
        scansMap = new HashMap<>();
        catalog = catalogLoader.load();

        updateTableMap();
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

    protected void updateTableMap()
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> databases = catalog.listDatabases();

        for (String databaseName : databases) {
            if (databasePattern.matcher(databaseName).matches()) {
                List<String> tables = catalog.listTables(databaseName);
                for (String tableName : tables) {
                    Identifier identifier = Identifier.create(databaseName, tableName);
                    if (shouldCompactTable(identifier, includingPattern, excludingPattern)
                            && (!tablesMap.containsKey(identifier))) {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.error(
                                    String.format(
                                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                                            table.getClass().getName()));
                            continue;
                        }
                        FileStoreTable fileStoreTable = (FileStoreTable) table;
                        if (fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE) {
                            LOG.info(
                                    String.format(
                                                    "the bucket mode of %s is unware. ",
                                                    identifier.getFullName())
                                            + "currently, the table with unware bucket mode is not support in combined mode.");
                            continue;
                        }
                        BucketsTable bucketsTable =
                                new BucketsTable(
                                                fileStoreTable,
                                                isStreaming,
                                                identifier.getDatabaseName())
                                        .copy(compactOptions(isStreaming));
                        tablesMap.put(identifier, bucketsTable);
                        scansMap.put(identifier, bucketsTable.newReadBuilder().newStreamScan());
                    }
                }
            }
        }
    }
}

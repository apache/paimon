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

package org.apache.paimon.flink.compact;

import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.shouldCompactTable;

/**
 * This class is responsible for implementing the scanning logic for the table of different type
 * buckets during compaction.
 *
 * @param <T> the result of scanning file :
 *     <ol>
 *       <li>Tuple2<{@link Split},String> for the table with multi buckets, such as dynamic or fixed
 *           bucket table.
 *       <li>{@link MultiTableUnawareAppendCompactionTask} for the table witch fixed single bucket
 *           ,such as unaware bucket table.
 *     </ol>
 */
public abstract class MultiTableScanBase<T> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableScanBase.class);
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;

    protected transient Catalog catalog;

    protected boolean isStreaming;

    public MultiTableScanBase(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming) {
        catalog = catalogLoader.load();

        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.isStreaming = isStreaming;
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
                            && (!checkTableScanned(identifier))) {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.error(
                                    String.format(
                                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                                            table.getClass().getName()));
                            continue;
                        }

                        FileStoreTable fileStoreTable = (FileStoreTable) table;
                        addScanTable(fileStoreTable, identifier);
                    }
                }
            }
        }
    }

    public ScanResult scanTable(ReaderOutput<T> ctx)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException {
        try {
            updateTableMap();
            List<T> tasks = doScan();

            tasks.forEach(ctx::collect);
            return tasks.isEmpty() ? ScanResult.IS_EMPTY : ScanResult.IS_NON_EMPTY;
        } catch (EndOfScanException esf) {
            LOG.info("Catching EndOfStreamException, the stream is finished.");
            return ScanResult.FINISHED;
        }
    }

    abstract List<T> doScan();

    /** Check if table has been scanned. */
    abstract boolean checkTableScanned(Identifier identifier);

    /** Add the scan table to the table map. */
    abstract void addScanTable(FileStoreTable fileStoreTable, Identifier identifier);

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    /** the result of table scanning. */
    public enum ScanResult {
        FINISHED,
        IS_EMPTY,
        IS_NON_EMPTY
    }
}

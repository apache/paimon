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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** {@link Action} test base. */
public abstract class ActionITCaseBase extends AbstractTestBase {

    protected String warehouse;
    protected String database;
    protected String tableName;
    protected String commitUser;
    protected StreamTableWrite write;
    protected StreamTableCommit commit;
    protected Catalog catalog;
    private long incrementalIdentifier;

    @BeforeEach
    public void before() throws IOException {
        warehouse = getTempDirPath();
        database = "default";
        tableName = "test_table_" + UUID.randomUUID();
        commitUser = UUID.randomUUID().toString();
        incrementalIdentifier = 0;
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
    }

    @AfterEach
    public void after() throws Exception {
        if (write != null) {
            write.close();
            write = null;
        }
        if (commit != null) {
            commit.close();
            commit = null;
        }
        catalog.close();
    }

    protected FileStoreTable createFileStoreTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        return createFileStoreTable(tableName, rowType, partitionKeys, primaryKeys, options);
    }

    protected FileStoreTable createFileStoreTable(
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createDatabase(database, true);
        catalog.createTable(
                identifier,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    protected FileStoreTable getFileStoreTable(String tableName) throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    protected GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    protected void writeData(GenericRow... data) throws Exception {
        for (GenericRow d : data) {
            write.write(d);
        }
        commit.commit(incrementalIdentifier, write.prepareCommit(true, incrementalIdentifier));
        incrementalIdentifier++;
    }

    protected List<String> getResult(TableRead read, List<Split> splits, RowType rowType)
            throws Exception {
        try (RecordReader<InternalRow> recordReader = read.createReader(splits)) {
            List<String> result = new ArrayList<>();
            recordReader.forEachRemaining(
                    row -> result.add(DataFormatTestUtil.internalRowToString(row, rowType)));
            return result;
        }
    }
}

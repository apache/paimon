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

import org.apache.flink.table.store.connector.util.AbstractTestBase;
import org.apache.flink.table.store.data.DataFormatTestUtil;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.catalog.CatalogUtils;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** {@link Action} test base. */
public class ActionITCaseBase extends AbstractTestBase {

    protected String warehouse;
    protected String database;
    protected String tableName;
    protected String commitUser;

    protected SnapshotManager snapshotManager;
    protected TableWrite write;
    protected TableCommit commit;

    private long incrementalIdentifier;

    @BeforeEach
    public void before() throws IOException {
        warehouse = getTempDirPath();
        database = "default";
        tableName = "test_table_" + UUID.randomUUID();
        commitUser = UUID.randomUUID().toString();
        incrementalIdentifier = 0;
    }

    @AfterEach
    public void after() throws Exception {
        if (write != null) {
            write.close();
        }
        if (commit != null) {
            commit.close();
        }
    }

    protected FileStoreTable createFileStoreTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        Path tablePath = CatalogUtils.path(warehouse, database, tableName);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        TableSchema tableSchema =
                schemaManager.commitNewVersion(
                        new UpdateSchema(rowType, partitionKeys, primaryKeys, options, ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
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
        RecordReader<InternalRow> recordReader = read.createReader(splits);
        List<String> result = new ArrayList<>();
        RecordReaderUtils.forEachRemaining(
                recordReader, row -> result.add(DataFormatTestUtil.rowDataToString(row, rowType)));
        return result;
    }
}

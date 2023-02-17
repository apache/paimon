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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.BinaryRowWriter;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.TraceableFileIO;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.FileIOFinder;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.reader.RecordReaderIterator;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypes;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Base test class for {@link SnapshotEnumerator} and related classes.
 *
 * <p>TODO: merge this class with FileStoreTableTestBase.
 */
public abstract class SnapshotEnumeratorTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                    new String[] {"pt", "a", "b"});

    @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;
    protected FileIO fileIO;
    protected String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        fileIO = FileIOFinder.find(tablePath);
        commitUser = UUID.randomUUID().toString();
    }

    protected GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    protected GenericRow rowDataWithKind(RowKind rowKind, Object... values) {
        return GenericRow.ofKind(rowKind, values);
    }

    protected BinaryRow binaryRow(int a) {
        BinaryRow b = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeInt(0, a);
        writer.complete();
        return b;
    }

    protected List<String> getResult(TableRead read, List<Split> splits) throws Exception {
        List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> read.createReader(split));
        }
        RecordReader<InternalRow> recordReader = ConcatRecordReader.create(readers);
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalRow rowData = iterator.next();
            result.add(rowDataToString(rowData));
        }
        iterator.close();
        return result;
    }

    protected String rowDataToString(InternalRow rowData) {
        return String.format(
                "%s %d|%d|%d",
                rowData.getRowKind().shortString(),
                rowData.getInt(0),
                rowData.getInt(1),
                rowData.getLong(2));
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        return createFileStoreTable(new Options());
    }

    protected FileStoreTable createFileStoreTable(Options conf) throws Exception {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "a"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema, conf);
    }
}

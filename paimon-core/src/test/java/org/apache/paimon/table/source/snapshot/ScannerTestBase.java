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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Base test class for {@link StartingScanner} and related classes.
 *
 * <p>TODO: merge this class with FileStoreTableTestBase.
 */
public abstract class ScannerTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                    new String[] {"pt", "a", "b"});

    protected @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;
    protected FileIO fileIO;
    protected String commitUser;
    protected FileStoreTable table;
    protected SnapshotReader snapshotReader;

    @BeforeEach
    public void before() throws Exception {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        fileIO = FileIOFinder.find(tablePath);
        commitUser = UUID.randomUUID().toString();
        table = createFileStoreTable();
        snapshotReader = table.newSnapshotReader();
    }

    protected void createAppendOnlyTable() throws Exception {
        tempDir = Files.createTempDirectory("junit");
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        fileIO = FileIOFinder.find(tablePath);
        table = createFileStoreTable(false);
        snapshotReader = table.newSnapshotReader();
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
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
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
        return createFileStoreTable(true, new Options(), tablePath);
    }

    protected FileStoreTable createFileStoreTable(Options conf) throws Exception {
        return createFileStoreTable(true, conf, tablePath);
    }

    protected FileStoreTable createFileStoreTable(boolean withPrimaryKeys) throws Exception {
        return createFileStoreTable(withPrimaryKeys, new Options(), tablePath);
    }

    protected FileStoreTable createFileStoreTable(
            boolean withPrimaryKeys, Options conf, Path tablePath) throws Exception {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        List<String> primaryKeys = new ArrayList<>();
        if (withPrimaryKeys) {
            primaryKeys = Arrays.asList("pt", "a");
        }
        if (!conf.contains(CoreOptions.BUCKET)) {
            conf.set(CoreOptions.BUCKET, 1);
            if (!withPrimaryKeys) {
                conf.set(CoreOptions.BUCKET_KEY, "a");
            }
        }
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                primaryKeys,
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                fileIO,
                tablePath,
                tableSchema,
                conf,
                CatalogEnvironment.empty(),
                new ExternalPathProvider());
    }

    protected List<Split> toSplits(List<DataSplit> dataSplits) {
        return new ArrayList<>(dataSplits);
    }
}

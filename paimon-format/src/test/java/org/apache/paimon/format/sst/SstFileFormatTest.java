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

package org.apache.paimon.format.sst;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Test class for {@link SstFileFormat}. */
public class SstFileFormatTest {
    private static final Logger LOG = LoggerFactory.getLogger(SstFileFormatTest.class);

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataField(0, "id", new IntType()),
                    new DataField(1, "name", new VarCharType()),
                    new DataField(2, "val1", new DoubleType()),
                    new DataField(3, "val2", new VarCharType()));
    private static final RowType KEY_TYPE =
            RowType.of(
                    new DataField(0, "id", new IntType()),
                    new DataField(1, "name", new VarCharType()));

    private static final RowType VALUE_TYPE =
            RowType.of(
                    new DataField(2, "val1", new DoubleType()),
                    new DataField(3, "val2", new VarCharType()));
    @TempDir java.nio.file.Path tempPath;

    protected FileIO fileIO;
    protected Path file;
    protected Path parent;
    protected Options options;

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
        this.options = new Options();
    }

    @Test
    public void testFullScan() throws Exception {
        final int recordNum = 1000000;
        writeData(recordNum);
        FileFormat format = createFormat();
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));

        FormatReaderFactory readerFactory;

        // 1. no projection
        readerFactory = format.createReaderFactory(ROW_TYPE, ROW_TYPE, null, KEY_TYPE, VALUE_TYPE);
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context); ) {
            assertScan(reader, null, ROW_TYPE, recordNum);
        }

        RowType projection =
                RowType.of(
                        new DataField(3, "val2", new VarCharType()),
                        new DataField(0, "id", new IntType()));
        // 2. with projection
        readerFactory =
                format.createReaderFactory(ROW_TYPE, projection, null, KEY_TYPE, VALUE_TYPE);
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context); ) {
            assertScan(reader, null, projection, recordNum);
        }
    }

    @Test
    public void testRandomAccess() throws Exception {
        final int recordNum = 100000;
        writeData(recordNum);
        FileFormat format = createFormat();

        FormatReaderFactory readerFactory;

        // 1. random selection without projection
        readerFactory = format.createReaderFactory(ROW_TYPE, ROW_TYPE, null, KEY_TYPE, VALUE_TYPE);
        for (int i = 0; i < 10; i++) {
            testRandomSelection(readerFactory, recordNum, 100, ROW_TYPE);
        }

        // 2. random selection with projection
        RowType projection =
                RowType.of(
                        new DataField(3, "val2", new VarCharType()),
                        new DataField(0, "id", new IntType()));
        readerFactory =
                format.createReaderFactory(ROW_TYPE, projection, null, KEY_TYPE, VALUE_TYPE);
        for (int i = 0; i < 10; i++) {
            testRandomSelection(readerFactory, recordNum, 100, projection);
        }
    }

    private void testRandomSelection(
            FormatReaderFactory readerFactory, int recordNum, int selectionNum, RowType projection)
            throws IOException {
        RoaringBitmap32 selection = new RoaringBitmap32();
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < selectionNum; i++) {
            selection.add(random.nextInt(recordNum));
        }

        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection);
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            assertScan(reader, selection, projection, (int) selection.getCardinality());
        }
    }

    private void assertScan(
            FileRecordReader<InternalRow> reader,
            RoaringBitmap32 selection,
            RowType projection,
            int num)
            throws IOException {
        long start = System.currentTimeMillis();
        ProjectedRow targetRow = ProjectedRow.from(projection, ROW_TYPE);
        FileRecordIterator<InternalRow> iter;
        Iterator<Integer> selectionIter = selection == null ? null : selection.iterator();

        int cnt = 0;
        while ((iter = reader.readBatch()) != null) {
            InternalRow record;
            while ((record = iter.next()) != null) {
                int target = selectionIter == null ? cnt : selectionIter.next();
                targetRow.replaceRow(constructRow(target));
                assertRowEqual(targetRow, record, projection);
                cnt++;
                if (cnt > num) {
                    throw new AssertionError(
                            String.format("Expected %s record, but at least found %s", num, cnt));
                }
            }
        }
        if (cnt != num) {
            throw new AssertionError(
                    String.format("Expected %s record, but only found %s", num, cnt));
        }
        LOG.info("Scan data cost {} ms", System.currentTimeMillis() - start);
    }

    private void writeData(int rowCount) throws IOException {
        long start = System.currentTimeMillis();
        FileFormat format = createFormat();
        FormatWriterFactory writerFactory =
                format.createWriterFactory(ROW_TYPE, KEY_TYPE, VALUE_TYPE);
        try (PositionOutputStream outputStream = fileIO.newOutputStream(file, true);
                FormatWriter writer = writerFactory.create(outputStream, "ZSTD"); ) {
            for (int i = 0; i < rowCount; i++) {
                writer.addElement(constructRow(i));
            }
        }
        LOG.info("Write {} rows cost {} ms", rowCount, System.currentTimeMillis() - start);
    }

    public void assertRowEqual(InternalRow expected, InternalRow actual, RowType rowType) {
        Assertions.assertEquals(expected.getFieldCount(), actual.getFieldCount());
        Assertions.assertEquals(expected.getRowKind(), actual.getRowKind());
        List<DataField> fields = rowType.getFields();
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            getters[i] = InternalRow.createFieldGetter(fields.get(i).type(), i);
        }
        for (InternalRow.FieldGetter getter : getters) {
            Assertions.assertEquals(getter.getFieldOrNull(expected), getter.getFieldOrNull(actual));
        }
    }

    private InternalRow constructRow(int index) {
        return GenericRow.of(
                index,
                BinaryString.fromString("name" + index),
                index * 1.1,
                BinaryString.fromString("value" + index));
    }

    private FileFormat createFormat() {
        SstFileFormatFactory factory = new SstFileFormatFactory();
        return factory.create(
                new FileFormatFactory.FormatContext(options, 0, 0, MemorySize.ofMebiBytes(1)));
    }
}

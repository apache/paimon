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

package org.apache.paimon.format.orc;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.OrcFormatReaderContext;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DecimalUtils;
import org.apache.paimon.utils.Projection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OrcReaderFactory}. */
class OrcReaderFactoryTest {

    /** Small batch size for test more boundary conditions. */
    protected static final int BATCH_SIZE = 9;

    private static final RowType FLAT_FILE_TYPE =
            RowType.builder()
                    .fields(
                            new DataType[] {
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.INT()
                            },
                            new String[] {
                                "_col0", "_col1", "_col2", "_col3", "_col4", "_col5", "_col6",
                                "_col7", "_col8"
                            })
                    .build();

    private static final RowType DECIMAL_FILE_TYPE =
            RowType.builder()
                    .fields(new DataType[] {new DecimalType(10, 5)}, new String[] {"_col0"})
                    .build();

    private static Path flatFile;
    private static Path decimalFile;

    @BeforeAll
    static void setupFiles(@TempDir java.nio.file.Path tmpDir) {
        flatFile = copyFileFromResource("test-data-flat.orc", tmpDir.resolve("test-data-flat.orc"));
        decimalFile =
                copyFileFromResource(
                        "test-data-decimal.orc", tmpDir.resolve("test-data-decimal.orc"));
    }

    @Test
    void testReadFileInSplits() throws IOException {
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        forEach(
                format,
                flatFile,
                row -> {
                    assertThat(row.isNullAt(0)).isFalse();
                    assertThat(row.isNullAt(1)).isFalse();
                    totalF0.addAndGet(row.getInt(0));
                    assertThat(row.getString(1).toString()).isNotNull();
                    cnt.incrementAndGet();
                });

        // check that all rows have been read
        assertThat(cnt.get()).isEqualTo(1920800);
        assertThat(totalF0.get()).isEqualTo(1844737280400L);
    }

    @Test
    void testReadFileWithSelectFields() throws IOException {
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {2, 0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        forEach(
                format,
                flatFile,
                row -> {
                    assertThat(row.isNullAt(0)).isFalse();
                    assertThat(row.isNullAt(1)).isFalse();
                    assertThat(row.isNullAt(2)).isFalse();
                    assertThat(row.getString(0).toString()).isNotNull();
                    totalF0.addAndGet(row.getInt(1));
                    assertThat(row.getString(2).toString()).isNotNull();
                    cnt.incrementAndGet();
                });

        // check that all rows have been read
        assertThat(cnt.get()).isEqualTo(1920800);
        assertThat(totalF0.get()).isEqualTo(1844737280400L);
    }

    @Test
    void testReadRowPosition() throws IOException {
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {2, 0, 1});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicLong totalF0 = new AtomicLong(0);

        LocalFileIO fileIO = new LocalFileIO();
        try (RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(fileIO, flatFile, fileIO.getFileSize(flatFile)))) {
            reader.forEachRemainingWithPosition(
                    (rowPosition, row) -> {
                        assertThat(row.isNullAt(0)).isFalse();
                        assertThat(row.isNullAt(1)).isFalse();
                        assertThat(row.isNullAt(2)).isFalse();
                        assertThat(row.getString(0).toString()).isNotNull();
                        totalF0.addAndGet(row.getInt(1));
                        assertThat(row.getString(2).toString()).isNotNull();
                        // check row position
                        assertThat(rowPosition).isEqualTo(cnt.get());
                        cnt.incrementAndGet();
                    });
        }
        // check that all rows have been read
        assertThat(cnt.get()).isEqualTo(1920800);
        assertThat(totalF0.get()).isEqualTo(1844737280400L);
    }

    @RepeatedTest(10)
    void testReadRowPositionWithRandomFilterAndPool() throws IOException {
        ArrayList<OrcFilters.Predicate> predicates = new ArrayList<>();
        int randomStart = new Random().nextInt(1920800);
        int randomPooSize = new Random().nextInt(3) + 1;
        predicates.add(
                new OrcFilters.Not(
                        new OrcFilters.LessThanEquals(
                                "_col0", PredicateLeaf.Type.LONG, randomStart)));
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {2, 0, 1}, predicates);

        AtomicBoolean isFirst = new AtomicBoolean(true);

        LocalFileIO localFileIO = new LocalFileIO();
        try (RecordReader<InternalRow> reader =
                format.createReader(
                        new OrcFormatReaderContext(
                                localFileIO,
                                flatFile,
                                localFileIO.getFileSize(flatFile),
                                randomPooSize))) {
            reader.forEachRemainingWithPosition(
                    (rowPosition, row) -> {
                        // check filter: _col0 > randomStart
                        // Note: the accuracy of filter is within flatFile's strip size
                        if (isFirst.get()) {
                            assertThat(randomStart - row.getInt(1)).isLessThan(5000);
                            isFirst.set(false);
                        }
                        // check row position
                        // Note: in flatFile, field _col0's value is row position + 1, we can use it
                        // to check row position
                        assertThat(rowPosition + 1).isEqualTo(row.getInt(1));
                    });
        }
    }

    @Test
    void testReadRowPositionWithTransformAndFilter() throws IOException {
        int randomPooSize = new Random().nextInt(3) + 1;
        OrcReaderFactory format = createFormat(FLAT_FILE_TYPE, new int[] {2, 0, 1});

        LocalFileIO localFileIO = new LocalFileIO();
        try (RecordReader<InternalRow> reader =
                format.createReader(
                        new OrcFormatReaderContext(
                                localFileIO,
                                flatFile,
                                localFileIO.getFileSize(flatFile),
                                randomPooSize))) {
            reader.transform(row -> row)
                    .filter(row -> row.getInt(1) % 123 == 0)
                    .forEachRemainingWithPosition(
                            (rowPosition, row) -> {
                                // check row position
                                // Note: in flatFile, field _col0's value is row position + 1, we
                                // can use it
                                // to check row position
                                assertThat(rowPosition + 1).isEqualTo(row.getInt(1));
                            });
        }
    }

    @Test
    void testReadDecimalTypeFile() throws IOException {
        OrcReaderFactory format = createFormat(DECIMAL_FILE_TYPE, new int[] {0});

        AtomicInteger cnt = new AtomicInteger(0);
        AtomicInteger nullCount = new AtomicInteger(0);

        forEach(
                format,
                decimalFile,
                row -> {
                    if (cnt.get() == 0) {
                        // validate first row
                        assertThat(row).isNotNull();
                        assertThat(row.getFieldCount()).isEqualTo(1);
                        assertThat(row.getDecimal(0, 10, 5))
                                .isEqualTo(DecimalUtils.castFrom(-1000.5d, 10, 5));
                    } else {
                        if (!row.isNullAt(0)) {
                            assertThat(row.getDecimal(0, 10, 5)).isNotNull();
                        } else {
                            nullCount.incrementAndGet();
                        }
                    }
                    cnt.incrementAndGet();
                });

        assertThat(cnt.get()).isEqualTo(6000);
        assertThat(nullCount.get()).isEqualTo(2000);
    }

    @Test
    public void testReadAllNull(@TempDir java.nio.file.Path tmpDir) throws IOException {
        int numRows = 2;

        // ARRAY
        RowType array =
                RowType.builder().field("array_int", DataTypes.ARRAY(DataTypes.INT())).build();
        Path tempPath = new Path(tmpDir.toUri() + UUID.randomUUID().toString() + ".orc");
        VectorizedColumnBatch batch = createAllNullBatch(tempPath, array, numRows);
        ArrayColumnVector arrayColumnVector = (ArrayColumnVector) batch.columns[0];
        ColumnVector dataColumnVector = arrayColumnVector.getColumnVector();
        for (int i = 0; i < numRows; i++) {
            assertThat(arrayColumnVector.isNullAt(i)).isTrue();
            assertThat(dataColumnVector.isNullAt(i)).isTrue();
        }

        // ARROW<ROW>
        RowType arrayRow =
                RowType.builder()
                        .field("array_row", DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT())))
                        .build();
        tempPath = new Path(tmpDir.toUri() + UUID.randomUUID().toString() + ".orc");
        batch = createAllNullBatch(tempPath, arrayRow, numRows);
        arrayColumnVector = (ArrayColumnVector) batch.columns[0];
        RowColumnVector nestedRowColumnVector =
                (RowColumnVector) arrayColumnVector.getColumnVector();
        ColumnVector nestedColumnVector = nestedRowColumnVector.getBatch().columns[0];
        for (int i = 0; i < numRows; i++) {
            assertThat(arrayColumnVector.isNullAt(i)).isTrue();
            assertThat(nestedRowColumnVector.isNullAt(i)).isTrue();
            assertThat(nestedColumnVector.isNullAt(i)).isTrue();
        }

        // MAP
        RowType map =
                RowType.builder()
                        .field("map", DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))
                        .build();
        tempPath = new Path(tmpDir.toUri() + UUID.randomUUID().toString() + ".orc");
        batch = createAllNullBatch(tempPath, map, numRows);
        MapColumnVector mapColumnVector = (MapColumnVector) batch.columns[0];
        ColumnVector keyColumnVector = mapColumnVector.getKeyColumnVector();
        ColumnVector valueColumnVector = mapColumnVector.getValueColumnVector();
        for (int i = 0; i < numRows; i++) {
            assertThat(mapColumnVector.isNullAt(i)).isTrue();
            assertThat(keyColumnVector.isNullAt(i)).isTrue();
            assertThat(valueColumnVector.isNullAt(i)).isTrue();
        }

        // ROW
        RowType row = RowType.builder().field("row", DataTypes.ROW(DataTypes.INT())).build();
        tempPath = new Path(tmpDir.toUri() + UUID.randomUUID().toString() + ".orc");
        batch = createAllNullBatch(tempPath, row, numRows);
        RowColumnVector rowColumnVector = (RowColumnVector) batch.columns[0];
        ColumnVector innerColumnVector = rowColumnVector.getBatch().columns[0];
        for (int i = 0; i < numRows; i++) {
            assertThat(mapColumnVector.isNullAt(i)).isTrue();
            assertThat(innerColumnVector.isNullAt(i)).isTrue();
        }
    }

    private VectorizedColumnBatch createAllNullBatch(Path tempPath, RowType rowType, int numRows)
            throws IOException {
        FileIO fileIO = LocalFileIO.create();

        OrcFileFormat orc =
                new OrcFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024));

        PositionOutputStream outputStream = fileIO.newOutputStream(tempPath, false);
        FormatWriter writer = orc.createWriterFactory(rowType).create(outputStream, null);
        for (int i = 0; i < numRows; i++) {
            writer.addElement(GenericRow.of((Object) null));
        }
        writer.finish();
        outputStream.close();

        RecordReader<InternalRow> reader =
                orc.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, tempPath, fileIO.getFileSize(tempPath)));

        return ((VectorizedRecordIterator) reader.readBatch()).batch();
    }

    protected OrcReaderFactory createFormat(RowType formatType, int[] selectedFields) {
        return createFormat(formatType, selectedFields, new ArrayList<>());
    }

    protected OrcReaderFactory createFormat(
            RowType formatType,
            int[] selectedFields,
            List<OrcFilters.Predicate> conjunctPredicates) {
        return new OrcReaderFactory(
                new Configuration(),
                Projection.of(selectedFields).project(formatType),
                conjunctPredicates,
                BATCH_SIZE);
    }

    private RecordReader<InternalRow> createReader(OrcReaderFactory format, Path split)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        return format.createReader(
                new FormatReaderContext(fileIO, split, fileIO.getFileSize(split)));
    }

    private void forEach(OrcReaderFactory format, Path file, Consumer<InternalRow> action)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        reader.forEachRemaining(action);
    }

    static Path copyFileFromResource(String resourceName, java.nio.file.Path file) {
        try (InputStream resource =
                OrcReaderFactoryTest.class
                        .getClassLoader()
                        .getResource(resourceName)
                        .openStream()) {
            Files.createDirectories(file.getParent());
            Files.copy(resource, file);
            return new Path(file.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

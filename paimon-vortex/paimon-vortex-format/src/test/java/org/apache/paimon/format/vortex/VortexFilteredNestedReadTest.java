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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads of nested string columns with a pushed filter. A filtered list can start at a non-zero
 * child offset, and a null list can still span child values.
 */
class VortexFilteredNestedReadTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("arr", DataTypes.ARRAY(DataTypes.STRING()))
                    .field(
                            "nested",
                            DataTypes.ROW(
                                    DataTypes.FIELD(0, "name", DataTypes.STRING()),
                                    DataTypes.FIELD(
                                            1, "tags", DataTypes.ARRAY(DataTypes.STRING()))))
                    .build();

    private static final int ROWS = 100;

    @TempDir java.nio.file.Path tempDir;

    private VortexFileFormat format;
    private FileIO fileIO;
    private Path file;

    @BeforeEach
    void writeFile() throws Exception {
        format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        fileIO = LocalFileIO.create();
        file = new Path(new Path(tempDir.toUri()), "nested-" + System.nanoTime());

        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(fileIO, file, "")) {
            for (int i = 0; i < ROWS; i++) {
                writer.addElement(row(i));
            }
        }
    }

    @Test
    void testUnfilteredRead() throws Exception {
        assertThat(readIds(ROW_TYPE, null)).isEqualTo(range(0, ROWS));
    }

    @Test
    void testFilteredRead() throws Exception {
        Predicate filter = new PredicateBuilder(ROW_TYPE).greaterOrEqual(0, 80);

        assertThat(readIds(ROW_TYPE, filter)).isEqualTo(range(80, ROWS));
    }

    @Test
    void testFilteredReadOfInnerRange() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        Predicate filter =
                PredicateBuilder.and(builder.greaterOrEqual(0, 33), builder.lessThan(0, 61));

        assertThat(readIds(ROW_TYPE, filter)).isEqualTo(range(33, 61));
    }

    @Test
    void testFilteredReadWithSelection() throws Exception {
        RowType projected = ROW_TYPE.project("id", "nested");
        Predicate filter = new PredicateBuilder(ROW_TYPE).greaterOrEqual(0, 80);

        assertThat(readIds(projected, filter)).isEqualTo(range(80, ROWS));
    }

    private List<Integer> readIds(RowType projected, Predicate filter) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(
                                ROW_TYPE,
                                projected,
                                filter == null ? null : Collections.singletonList(filter))
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            FileRecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    int id = row.getInt(0);
                    assertRow(projected, row, id);
                    ids.add(id);
                }
                batch.releaseBatch();
            }
        }
        return ids;
    }

    private static void assertRow(RowType projected, InternalRow row, int id) {
        int nestedPos = projected.getFieldIndex("nested");
        int arrPos = projected.getFieldIndex("arr");

        if (arrPos >= 0) {
            if (id % 5 == 0) {
                assertThat(row.isNullAt(arrPos)).isTrue();
            } else {
                assertThat(strings(row.getArray(arrPos))).isEqualTo(values(id));
            }
        }

        InternalRow nested = row.getRow(nestedPos, 2);
        assertThat(nested.getString(0).toString()).isEqualTo(name(id));
        if (id % 3 == 0) {
            assertThat(nested.isNullAt(1)).isTrue();
        } else {
            assertThat(strings(nested.getArray(1))).isEqualTo(values(id + 1));
        }
    }

    private static GenericRow row(int i) {
        return GenericRow.of(
                i,
                i % 5 == 0 ? null : array(values(i)),
                GenericRow.of(
                        BinaryString.fromString(name(i)),
                        i % 3 == 0 ? null : array(values(i + 1))));
    }

    /** Values of 0 to 3 strings, some longer than the 12-byte inline limit of Arrow views. */
    private static List<String> values(int i) {
        List<String> values = new ArrayList<>();
        for (int j = 0; j < i % 4; j++) {
            values.add(j % 2 == 0 ? "v" + i + "_" + j : "a-long-value-" + i + "-" + j);
        }
        return values;
    }

    private static String name(int i) {
        return "name-that-is-not-inline-" + i;
    }

    private static GenericArray array(List<String> values) {
        Object[] strings = new Object[values.size()];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = BinaryString.fromString(values.get(i));
        }
        return new GenericArray(strings);
    }

    private static List<String> strings(InternalArray array) {
        String[] strings = new String[array.size()];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = array.getString(i).toString();
        }
        return Arrays.asList(strings);
    }

    private static List<Integer> range(int from, int to) {
        List<Integer> ids = new ArrayList<>();
        for (int i = from; i < to; i++) {
            ids.add(i);
        }
        return ids;
    }
}

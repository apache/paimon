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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for reading Parquet files that use legacy list encodings, as described by the
 * backward-compatibility rules in the Parquet spec.
 */
public class ParquetLegacyListReadTest {

    private static final int ROW_COUNT = 10;

    @TempDir public File folder;

    /** Backward-compatibility Rule 1: a repeated primitive field is itself the element type. */
    @Test
    public void testReadRepeatedPrimitiveElementList() throws Exception {
        // optional group my_list (LIST) { repeated int32 element; }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .repeated(INT32)
                                .named("element")
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    array.append("element", i * 10 + j);
                }
                writer.write(row);
            }
        }

        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0, "my_list", new ArrayType(new IntType())))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            assertThat(array.getInt(j)).isEqualTo(index * 10 + j);
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * Backward-compatibility Rule 2: a repeated group with multiple fields is itself the element
     * type.
     */
    @Test
    public void testReadRepeatedStructElementList() throws Exception {
        // optional group my_list (LIST) { repeated group element { optional int32 x; optional int32
        // y; } }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .optional(INT32)
                                                .named("x")
                                                .optional(INT32)
                                                .named("y")
                                                .named("element"))
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    array.addGroup("element").append("x", i + j).append("y", i * j);
                }
                writer.write(row);
            }
        }

        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "x", new IntType()),
                                new DataField(1, "y", new IntType())));
        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0, "my_list", new ArrayType(elementType)))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            InternalRow element = array.getRow(j, 2);
                            assertThat(element.getInt(0)).isEqualTo(index + j);
                            assertThat(element.getInt(1)).isEqualTo(index * j);
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * Backward-compatibility Rule 2 with projection: a repeated group with multiple fields is the
     * element type, and projecting it to a single field must not reclassify the clipped element
     * group as a Rule 5 wrapper.
     */
    @Test
    public void testReadStructElementWithProjection() throws Exception {
        // optional group my_list (LIST) { repeated group element { optional int32 x; optional int32
        // y; } }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .optional(INT32)
                                                .named("x")
                                                .optional(INT32)
                                                .named("y")
                                                .named("element"))
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    array.addGroup("element").append("x", i + j).append("y", i * j);
                }
                writer.write(row);
            }
        }

        RowType elementType =
                new RowType(Collections.singletonList(new DataField(0, "x", new IntType())));
        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0, "my_list", new ArrayType(elementType)))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            assertThat(array.getRow(j, 1).getInt(0)).isEqualTo(index + j);
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * Backward-compatibility Rule 3: a repeated group whose single child is also repeated is the
     * element type, and the repeated child is a nested list. Reading {@code [[8, 9], ...]} must
     * return every inner value, not just the first one.
     */
    @Test
    public void testReadNestedLegacyList() throws Exception {
        // optional group my_list (LIST) { repeated group element { repeated int32 array; } }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .repeated(INT32)
                                                .named("array")
                                                .named("array"))
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    Group element = array.addGroup("array");
                    element.append("array", i * 10 + j);
                    element.append("array", i * 10 + j + 100);
                }
                writer.write(row);
            }
        }

        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "my_list",
                                                new ArrayType(new ArrayType(new IntType()))))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            InternalArray nested = array.getArray(j);
                            assertThat(nested.size()).isEqualTo(2);
                            assertThat(nested.getInt(0)).isEqualTo(index * 10 + j);
                            assertThat(nested.getInt(1)).isEqualTo(index * 10 + j + 100);
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * Backward-compatibility Rule 4: a repeated group named {@code "array"} with one field is the
     * element type.
     */
    @Test
    public void testReadArrayNamedGroupElementList() throws Exception {
        // optional group my_list (LIST) { repeated group array { optional int32 foo; } }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .optional(INT32)
                                                .named("foo")
                                                .named("array"))
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    array.addGroup("array").append("foo", i * 10 + j);
                }
                writer.write(row);
            }
        }

        RowType elementType =
                new RowType(Collections.singletonList(new DataField(0, "foo", new IntType())));
        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0, "my_list", new ArrayType(elementType)))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            assertThat(array.getRow(j, 1).getInt(0)).isEqualTo(index * 10 + j);
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * Backward-compatibility Rule 4: a repeated group named {@code "<list>_tuple"} with one field
     * is the element type.
     */
    @Test
    public void testReadListTupleNamedGroupElementList() throws Exception {
        // optional group my_list (LIST) { repeated group my_list_tuple { required binary str
        // (STRING); } }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .required(BINARY)
                                                .as(LogicalTypeAnnotation.stringType())
                                                .named("str")
                                                .named("my_list_tuple"))
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    array.addGroup("my_list_tuple")
                            .append("str", Binary.fromString("v" + (i * 10 + j)));
                }
                writer.write(row);
            }
        }

        RowType elementType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "str", new VarCharType(VarCharType.MAX_LENGTH))));
        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0, "my_list", new ArrayType(elementType)))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            assertThat(array.getRow(j, 1).getString(0))
                                    .isEqualTo(BinaryString.fromString("v" + (index * 10 + j)));
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * Backward-compatibility Rule 5: a repeated group with a single non-repeated child is a
     * wrapper, so the child is the element (Hive bag style).
     */
    @Test
    public void testReadElementWrappedList() throws Exception {
        // optional group my_list (LIST) { repeated group element { optional int32 num; } }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.listType())
                                .addField(
                                        Types.buildGroup(Type.Repetition.REPEATED)
                                                .optional(INT32)
                                                .named("num")
                                                .named("element"))
                                .named("my_list"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                Group array = row.addGroup("my_list");
                int size = i % 3 + 1;
                for (int j = 0; j < size; j++) {
                    array.addGroup("element").append("num", i * 10 + j);
                }
                writer.write(row);
            }
        }

        AtomicInteger i = new AtomicInteger();
        try (RecordReader<InternalRow> reader =
                createReader(
                        path,
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0, "my_list", new ArrayType(new IntType())))))) {
            reader.forEachRemaining(
                    row -> {
                        int index = i.getAndIncrement();
                        InternalArray array = row.getArray(0);
                        int size = index % 3 + 1;
                        assertThat(array.size()).isEqualTo(size);
                        for (int j = 0; j < size; j++) {
                            assertThat(array.getInt(j)).isEqualTo(index * 10 + j);
                        }
                    });
        }
        assertThat(i.get()).isEqualTo(ROW_COUNT);
    }

    /**
     * A plain struct group is neither LIST-annotated nor a legacy nested list, so reading it as an
     * ARRAY must fail fast in schema clipping instead of silently producing undefined data.
     */
    @Test
    public void testReadNonListGroupAsArrayFails() throws Exception {
        // optional group arr { optional int32 x; optional int32 y; }
        MessageType schema =
                new MessageType(
                        "origin-parquet",
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                .addField(
                                        Types.buildGroup(Type.Repetition.OPTIONAL)
                                                .optional(INT32)
                                                .named("x")
                                                .optional(INT32)
                                                .named("y")
                                                .named("tuple"))
                                .named("arr"));
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        try (ParquetWriter<Group> writer = createWriter(path, schema)) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (int i = 0; i < ROW_COUNT; i++) {
                Group row = factory.newGroup();
                row.addGroup("arr").addGroup("tuple").append("x", i).append("y", i * 2);
                writer.write(row);
            }
        }

        RowType readType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "arr", new ArrayType(new IntType()))));
        assertThatThrownBy(() -> createReader(path, readType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot read Parquet group 'arr' as an ARRAY");
    }

    private ParquetWriter<Group> createWriter(Path path, MessageType schema) throws IOException {
        Configuration conf = new Configuration();
        return ExampleParquetWriter.builder(
                        HadoopOutputFile.fromPath(
                                new org.apache.hadoop.fs.Path(path.toString()), conf))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withConf(conf)
                .withType(schema)
                .build();
    }

    private RecordReader<InternalRow> createReader(Path path, RowType readType) throws IOException {
        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), readType, 1024, null);
        LocalFileIO fileIO = new LocalFileIO();
        return factory.createReader(
                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), null, null));
    }
}

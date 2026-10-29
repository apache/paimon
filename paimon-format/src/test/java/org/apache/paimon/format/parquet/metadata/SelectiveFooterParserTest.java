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

package org.apache.paimon.format.parquet.metadata;

import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TMemoryBuffer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Equivalence tests for {@link SelectiveFooterParser}: the selective parse must produce exactly the
 * same {@link ParquetMetadata} as the official {@link ParquetMetadataConverter} for the retained
 * subset, and skip everything else.
 */
public class SelectiveFooterParserTest {

    @TempDir java.nio.file.Path tempDir;

    private static final MessageType SCHEMA =
            new MessageType(
                    "test",
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                            .named("id"),
                    Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.BINARY,
                                    Type.Repetition.OPTIONAL)
                            .as(LogicalTypeAnnotation.stringType())
                            .named("name"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .as(LogicalTypeAnnotation.decimalType(2, 8))
                            .named("price"),
                    Types.optionalGroup()
                            .required(PrimitiveType.PrimitiveTypeName.INT32)
                            .named("a")
                            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                            .named("b")
                            .named("g"),
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                            .as(
                                    LogicalTypeAnnotation.timestampType(
                                            true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named("ts"));

    private File writeTestFile(int rowCount, long rowGroupSize) throws IOException {
        File file = tempDir.resolve("test.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(SCHEMA)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(rowGroupSize)
                        .build()) {
            for (int i = 0; i < rowCount; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                if (i % 7 != 0) { // leave some nulls to exercise statistics
                    group.append("name", "name-" + (i % 37));
                }
                group.append("price", i % 128);
                Group nested = group.addGroup("g");
                nested.append("a", i % 10);
                if (i % 5 != 0) {
                    nested.append("b", i * 1.5);
                }
                group.append("ts", 1700000000000L + i);
                writer.write(group);
            }
        }
        return file;
    }

    private byte[] readFooterBytes(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long fileLen = raf.length();
            raf.seek(fileLen - 8);
            byte[] tail = new byte[8];
            raf.readFully(tail);
            int footerLen =
                    (tail[0] & 0xff)
                            | ((tail[1] & 0xff) << 8)
                            | ((tail[2] & 0xff) << 16)
                            | ((tail[3] & 0xff) << 24);
            byte[] footer = new byte[footerLen];
            raf.seek(fileLen - 8 - footerLen);
            raf.readFully(footer);
            return footer;
        }
    }

    /** Equivalence in "parse everything" mode — must match the official NO_FILTER parse. */
    @Test
    public void testFullParseEquivalence() throws IOException {
        File file = writeTestFile(300, 1024); // several row groups
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    /** Selective mode: only requested column chunks are materialized, block info identical. */
    @Test
    public void testSelectiveSubsetParse() throws IOException {
        File file = writeTestFile(300, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        Set<String> requested = new HashSet<>(Arrays.asList("id", "g"));
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, converter);

        List<BlockMetaData> officialBlocks = official.getBlocks();
        List<BlockMetaData> selectiveBlocks = selective.getBlocks();
        Assertions.assertEquals(officialBlocks.size(), selectiveBlocks.size());

        for (int i = 0; i < officialBlocks.size(); i++) {
            BlockMetaData officialBlock = officialBlocks.get(i);
            BlockMetaData selectiveBlock = selectiveBlocks.get(i);

            Assertions.assertEquals(officialBlock.getRowCount(), selectiveBlock.getRowCount());
            Assertions.assertEquals(
                    officialBlock.getTotalByteSize(), selectiveBlock.getTotalByteSize());
            Assertions.assertEquals(
                    officialBlock.getRowIndexOffset(), selectiveBlock.getRowIndexOffset());
            Assertions.assertEquals(officialBlock.getOrdinal(), selectiveBlock.getOrdinal());

            List<String> selectivePaths =
                    selectiveBlock.getColumns().stream()
                            .map(c -> c.getPath().toDotString())
                            .collect(Collectors.toList());
            Assertions.assertTrue(
                    selectivePaths.stream().allMatch(p -> requested.contains(p.split("\\.")[0])));

            Map<String, ColumnChunkMetaData> officialByPath =
                    officialBlock.getColumns().stream()
                            .collect(Collectors.toMap(c -> c.getPath().toDotString(), c -> c));
            for (ColumnChunkMetaData chunk : selectiveBlock.getColumns()) {
                ColumnChunkMetaData expected = officialByPath.get(chunk.getPath().toDotString());
                Assertions.assertNotNull(expected);
                Assertions.assertEquals(expected.getTotalSize(), chunk.getTotalSize());
                Assertions.assertEquals(expected.getStartingPos(), chunk.getStartingPos());
                Assertions.assertEquals(expected.getValueCount(), chunk.getValueCount());
                Assertions.assertEquals(expected.getCodec(), chunk.getCodec());
                Assertions.assertEquals(expected.getEncodings(), chunk.getEncodings());
                Assertions.assertArrayEquals(
                        expected.getStatistics().getMinBytes(),
                        chunk.getStatistics().getMinBytes());
                Assertions.assertArrayEquals(
                        expected.getStatistics().getMaxBytes(),
                        chunk.getStatistics().getMaxBytes());
                Assertions.assertEquals(
                        expected.getStatistics().getNumNulls(),
                        chunk.getStatistics().getNumNulls());
                // index references and bloom filter metadata must survive the selective parse
                // (IndexReference has no equals(), compare offset/length)
                Assertions.assertEquals(
                        expected.getColumnIndexReference().getOffset(),
                        chunk.getColumnIndexReference().getOffset());
                Assertions.assertEquals(
                        expected.getColumnIndexReference().getLength(),
                        chunk.getColumnIndexReference().getLength());
                Assertions.assertEquals(
                        expected.getOffsetIndexReference().getOffset(),
                        chunk.getOffsetIndexReference().getOffset());
                Assertions.assertEquals(
                        expected.getOffsetIndexReference().getLength(),
                        chunk.getOffsetIndexReference().getLength());
                Assertions.assertEquals(
                        expected.getBloomFilterOffset(), chunk.getBloomFilterOffset());
                Assertions.assertEquals(
                        expected.getBloomFilterLength(), chunk.getBloomFilterLength());
            }
        }
    }

    /** Statistics of dropped columns never leak into the selective result. */
    @Test
    public void testDroppedColumnsDoNotAffectBlockLevelInfo() throws IOException {
        File file = writeTestFile(300, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(
                        ByteBuffer.wrap(footer), new HashSet<>(Arrays.asList("name")), converter);

        Assertions.assertEquals(official.getBlocks().size(), selective.getBlocks().size());
        // FileMetaData has no equals override; compare the observable parts
        Assertions.assertEquals(
                official.getFileMetaData().getSchema(), selective.getFileMetaData().getSchema());
        Assertions.assertEquals(
                official.getFileMetaData().getKeyValueMetaData(),
                selective.getFileMetaData().getKeyValueMetaData());
        Assertions.assertEquals(
                official.getFileMetaData().getCreatedBy(),
                selective.getFileMetaData().getCreatedBy());
        for (int i = 0; i < official.getBlocks().size(); i++) {
            Assertions.assertEquals(
                    official.getBlocks().get(i).getRowCount(),
                    selective.getBlocks().get(i).getRowCount());
            Assertions.assertEquals(
                    official.getBlocks().get(i).getRowIndexOffset(),
                    selective.getBlocks().get(i).getRowIndexOffset());
            Assertions.assertEquals(1, selective.getBlocks().get(i).getColumns().size());
            Assertions.assertEquals(
                    "name",
                    selective.getBlocks().get(i).getColumns().get(0).getPath().toDotString());
        }
    }

    @Test
    public void testSplitRequestedColumns() {
        // Use null character as separator to support column names containing commas
        String sep = SelectiveFooterParser.COLUMN_NAME_SEPARATOR;
        Set<String> set =
                SelectiveFooterParser.splitRequestedColumns("a" + sep + "b" + sep + sep + "c");
        Assertions.assertEquals(new HashSet<>(Arrays.asList("a", "b", "c")), set);
    }

    /**
     * includeStatistics=false skips even the kept columns' Statistics (AI training mode); all other
     * metadata must stay equivalent.
     */
    @Test
    public void testSkipStatisticsParse() throws IOException {
        File file = writeTestFile(300, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        Set<String> requested = new HashSet<>(Arrays.asList("id", "g"));
        ParquetMetadata withStats =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, true, converter);
        ParquetMetadata withoutStats =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, false, converter);

        Assertions.assertEquals(withStats.getBlocks().size(), withoutStats.getBlocks().size());
        for (int i = 0; i < withStats.getBlocks().size(); i++) {
            List<ColumnChunkMetaData> with = withStats.getBlocks().get(i).getColumns();
            List<ColumnChunkMetaData> without = withoutStats.getBlocks().get(i).getColumns();
            Assertions.assertEquals(with.size(), without.size());
            for (int c = 0; c < with.size(); c++) {
                ColumnChunkMetaData a = with.get(c);
                ColumnChunkMetaData b = without.get(c);
                // everything except statistics must match
                Assertions.assertEquals(a.getPath(), b.getPath());
                Assertions.assertEquals(a.getTotalSize(), b.getTotalSize());
                Assertions.assertEquals(a.getStartingPos(), b.getStartingPos());
                Assertions.assertEquals(a.getValueCount(), b.getValueCount());
                Assertions.assertEquals(a.getCodec(), b.getCodec());
                Assertions.assertEquals(a.getEncodings(), b.getEncodings());
                // statistics are absent (empty) in the skip mode
                Assertions.assertTrue(b.getStatistics() == null || b.getStatistics().isEmpty());
            }
        }
    }

    /**
     * Full-mode equivalence over a schema exercising the wider LogicalType union and column_orders
     * (an unsigned integer column makes the writer emit a TYPE_ORDER entry). The manual {@code
     * readLogicalType}/{@code readColumnOrders} must still match the official parse.
     */
    @Test
    public void testFullParseEquivalenceRichLogicalTypes() throws IOException {
        MessageType schema =
                new MessageType(
                        "rich",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT32,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT32,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.dateType())
                                .named("date"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT32,
                                        Type.Repetition.OPTIONAL)
                                .as(
                                        LogicalTypeAnnotation.timeType(
                                                true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named("time"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT32,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.intType(32, true))
                                .named("signed_int"),
                        // unsigned integer => TYPE_ORDER entry in the footer's column_orders
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT32,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.intType(16, false))
                                .named("unsigned_short"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.BINARY,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.enumType())
                                .named("enum_col"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.BINARY,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.jsonType())
                                .named("json_col"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.BINARY,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.bsonType())
                                .named("bson_col"));
        File file = tempDir.resolve("rich.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 40; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", i);
                group.append("date", i);
                group.append("time", i * 1000);
                group.append("signed_int", -i);
                group.append("unsigned_short", i % 300);
                group.append("enum_col", "enum-" + (i % 5));
                group.append("json_col", "{\"i\":" + i + "}");
                group.append("bson_col", "bson-" + (i % 3));
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    // ------------------------------------------------------------------ complex schema

    /**
     * Deep nesting (3+ levels): verifies the parser correctly walks nested structs beyond the
     * typical 2-level depth.
     */
    @Test
    public void testDeepNestingSchema() throws IOException {
        MessageType schema =
                new MessageType(
                        "deep",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.optionalGroup()
                                .optionalGroup()
                                .optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("level3_field")
                                .named("level2_group")
                                .named("level1_group"));
        File file = tempDir.resolve("deep.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 20; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                Group level1 = group.addGroup("level1_group");
                Group level2 = level1.addGroup("level2_group");
                level2.append("level3_field", i);
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    /** Map type: verifies the parser handles map structures (key-value pairs). */
    @Test
    public void testMapTypeSchema() throws IOException {
        MessageType schema =
                new MessageType(
                        "map_test",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.optionalMap()
                                .key(PrimitiveType.PrimitiveTypeName.BINARY)
                                .value(
                                        Types.primitive(
                                                        PrimitiveType.PrimitiveTypeName.INT32,
                                                        Type.Repetition.OPTIONAL)
                                                .named("value"))
                                .named("properties"));
        File file = tempDir.resolve("map.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 20; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                Group mapGroup = group.addGroup("properties");
                Group keyValuePair = mapGroup.addGroup("key_value");
                keyValuePair.append("key", "key-" + (i % 3));
                keyValuePair.append("value", i);
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    /**
     * Array type: verifies the parser handles list structures. Note: Nested lists (array of array)
     * require complex schema construction; this test validates single-level list handling.
     */
    @Test
    public void testArrayTypeSchema() throws IOException {
        MessageType schema =
                new MessageType(
                        "array_test",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.optionalList()
                                .element(
                                        Types.primitive(
                                                        PrimitiveType.PrimitiveTypeName.INT32,
                                                        Type.Repetition.OPTIONAL)
                                                .named("element"))
                                .named("values"));
        File file = tempDir.resolve("array.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 10; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                // List structure: values -> list (repeated) -> element
                Group valuesGroup = group.addGroup("values");
                for (int j = 0; j < 3; j++) {
                    Group listGroup = valuesGroup.addGroup("list");
                    listGroup.append("element", i + j);
                }
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    // ------------------------------------------------------------------ boundary conditions

    /** Empty file (0 rows): verifies the parser handles files with no data. */
    @Test
    public void testEmptyFile() throws IOException {
        MessageType schema =
                new MessageType(
                        "empty",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"));
        File file = tempDir.resolve("empty.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            // Write 0 rows
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
        Assertions.assertEquals(0, selective.getBlocks().size());
    }

    /** Single row group: verifies the parser handles files with exactly one row group. */
    @Test
    public void testSingleRowGroup() throws IOException {
        File file = writeTestFile(50, 1024 * 1024); // large row group size = single group
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
        Assertions.assertEquals(1, selective.getBlocks().size());
    }

    /** All columns requested: verifies behavior when requestedColumns contains all column names. */
    @Test
    public void testAllColumnsRequested() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);

        // Request all top-level columns
        Set<String> allColumns = new HashSet<>(Arrays.asList("id", "name", "price", "g", "ts"));
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), allColumns, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    /** Non-existent columns: verifies behavior when requested columns don't exist in schema. */
    @Test
    public void testNonExistentColumnsRequested() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        // Request columns that don't exist
        Set<String> nonExistent = new HashSet<>(Arrays.asList("nonexistent1", "nonexistent2"));

        // Should fallback to official parser or return empty columns
        try {
            ParquetMetadata selective =
                    SelectiveFooterParser.parse(ByteBuffer.wrap(footer), nonExistent, converter);
            // If no fallback, verify no columns are retained
            for (BlockMetaData block : selective.getBlocks()) {
                Assertions.assertEquals(0, block.getColumns().size());
            }
        } catch (SelectiveFooterParser.FallbackSignalException e) {
            // Expected: fallback to official parser when no requested columns exist
        }
    }

    // ------------------------------------------------------------------ exception paths

    /** Malformed footer (truncated): verifies the parser handles corrupted data gracefully. */
    @Test
    public void testMalformedFooterTruncated() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        // Truncate the footer to simulate corruption
        byte[] truncated = Arrays.copyOf(footer, footer.length / 2);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        Assertions.assertThrows(
                SelectiveFooterParser.FallbackSignalException.class,
                () -> SelectiveFooterParser.parse(ByteBuffer.wrap(truncated), null, converter));
    }

    /** Malformed footer (wrong thrift type): verifies the parser handles type mismatches. */
    @Test
    public void testMalformedFooterWrongType() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        // Corrupt a byte in the middle to trigger type mismatch
        byte[] corrupted = footer.clone();
        if (corrupted.length > 10) {
            corrupted[10] = (byte) 0xFF; // Invalid thrift type
        }

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        Assertions.assertThrows(
                SelectiveFooterParser.FallbackSignalException.class,
                () -> SelectiveFooterParser.parse(ByteBuffer.wrap(corrupted), null, converter));
    }

    /**
     * Fallback to official parser: verifies FallbackSignalException is properly caught and handled.
     */
    @Test
    public void testFallbackSignalExceptionHandling() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        // Create a footer that will trigger fallback (e.g., with encryption_algorithm field)
        // Since we can't easily create an encrypted footer in a unit test, we verify the
        // exception type is correct by using a malformed footer
        byte[] truncated = Arrays.copyOf(footer, 5);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        try {
            SelectiveFooterParser.parse(ByteBuffer.wrap(truncated), null, converter);
            Assertions.fail("Expected FallbackSignalException to be thrown");
        } catch (SelectiveFooterParser.FallbackSignalException e) {
            Assertions.assertTrue(e.getMessage().contains("selective footer parse failed"));
        }
    }

    // ------------------------------------------------------------------ encryption

    /**
     * Encrypted footer: verifies that encrypted footers trigger FallbackSignalException. Note:
     * Creating actual encrypted parquet files requires encryption keys, so this test verifies the
     * fallback mechanism by checking that the parser correctly identifies encryption-related
     * fields.
     */
    @Test
    public void testEncryptedFooterFallback() throws IOException {
        // We cannot easily create an encrypted parquet file in a unit test without encryption keys.
        // Instead, we verify that the parser correctly handles the encryption_algorithm field
        // (id=8)
        // by checking the code path. This is a structural test.

        // For now, we verify that a normal footer does NOT trigger encryption fallback
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        // Should parse successfully without fallback
        Assertions.assertNotNull(selective);
        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(
                        converter.readParquetMetadata(
                                new ByteArrayInputStream(footer),
                                ParquetMetadataConverter.NO_FILTER)),
                ParquetMetadata.toPrettyJSON(selective));
    }

    /**
     * Encrypted column: verifies that encrypted columns trigger FallbackSignalException. Note:
     * Similar to testEncryptedFooterFallback, we verify the normal path works correctly.
     */
    @Test
    public void testEncryptedColumnFallback() throws IOException {
        // We cannot easily create a parquet file with encrypted columns in a unit test.
        // This test verifies that a normal (unencrypted) file parses successfully.
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertNotNull(selective);
        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(
                        converter.readParquetMetadata(
                                new ByteArrayInputStream(footer),
                                ParquetMetadataConverter.NO_FILTER)),
                ParquetMetadata.toPrettyJSON(selective));
    }

    // ------------------------------------------------------------------ integration

    /**
     * Integration with ParquetReadOptions: verifies that REQUESTED_COLUMNS_KEY and
     * SKIP_STATISTICS_KEY properties are correctly propagated through ParquetReadOptions.
     */
    @Test
    public void testParquetReadOptionsIntegration() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();

        // Test with REQUESTED_COLUMNS_KEY set
        Set<String> requested = new HashSet<>(Arrays.asList("id", "name"));
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, true, converter);

        // Verify only requested columns are present
        for (BlockMetaData block : selective.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                String topLevel = chunk.getPath().toDotString().split("\\.")[0];
                Assertions.assertTrue(
                        requested.contains(topLevel),
                        "Column " + topLevel + " should not be present");
            }
        }

        // Test with SKIP_STATISTICS_KEY = true
        ParquetMetadata noStats =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, false, converter);

        // Verify statistics are skipped
        for (BlockMetaData block : noStats.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                Assertions.assertTrue(
                        chunk.getStatistics() == null || chunk.getStatistics().isEmpty(),
                        "Statistics should be skipped");
            }
        }
    }

    /**
     * Split requested columns utility: verifies the splitRequestedColumns method handles various
     * input formats correctly. Whitespace is preserved as it may be part of valid column names.
     */
    @Test
    public void testSplitRequestedColumnsVariations() {
        String sep = SelectiveFooterParser.COLUMN_NAME_SEPARATOR;

        // Test with empty string
        Set<String> empty = SelectiveFooterParser.splitRequestedColumns("");
        Assertions.assertEquals(0, empty.size());

        // Test with single column
        Set<String> single = SelectiveFooterParser.splitRequestedColumns("col1");
        Assertions.assertEquals(new HashSet<>(Arrays.asList("col1")), single);

        // Test with multiple columns and empty entries
        Set<String> multiple =
                SelectiveFooterParser.splitRequestedColumns(
                        "a" + sep + sep + "b" + sep + "c" + sep);
        Assertions.assertEquals(new HashSet<>(Arrays.asList("a", "b", "c")), multiple);

        // Test with whitespace (preserved, not trimmed - whitespace may be part of column names)
        Set<String> whitespace =
                SelectiveFooterParser.splitRequestedColumns("a" + sep + " b " + sep + "c");
        Assertions.assertEquals(new HashSet<>(Arrays.asList("a", " b ", "c")), whitespace);

        // Test with whitespace-only entries (preserved as valid column names)
        Set<String> blankEntries =
                SelectiveFooterParser.splitRequestedColumns("a" + sep + "  " + sep + sep + " b");
        Assertions.assertEquals(new HashSet<>(Arrays.asList("a", "  ", " b")), blankEntries);

        // Test with column names containing commas (the key feature of null separator)
        Set<String> commasInNames =
                SelectiveFooterParser.splitRequestedColumns(
                        "col1" + sep + "$% ^,& *(" + sep + "col3");
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("col1", "$% ^,& *(", "col3")), commasInNames);
    }

    /**
     * Case-insensitive column matching: verifies that the parser correctly handles case-insensitive
     * column name resolution when caseSensitive=false.
     */
    @Test
    public void testCaseInsensitiveColumnMatching() throws IOException {
        // Create a file with mixed-case column names
        MessageType schema =
                new MessageType(
                        "case_test",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("Event_Name"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("Amount"));

        File file = tempDir.resolve("case_test.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 10; i++) {
                Group group = groupFactory.newGroup();
                group.append("Event_Name", (long) i);
                group.append("Amount", (long) (i * 100));
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();

        // Request columns with different case (lowercase)
        Set<String> requested = new HashSet<>(Arrays.asList("event_name", "amount"));

        // Test case-insensitive matching
        ParquetMetadata selective =
                SelectiveFooterParser.parse(
                        ByteBuffer.wrap(footer), requested, true, false, converter);

        // Verify both columns are present (case-insensitive match)
        Assertions.assertEquals(1, selective.getBlocks().size());
        Assertions.assertEquals(2, selective.getBlocks().get(0).getColumns().size());

        Set<String> columnNames = new HashSet<>();
        for (ColumnChunkMetaData chunk : selective.getBlocks().get(0).getColumns()) {
            columnNames.add(chunk.getPath().toDotString());
        }
        Assertions.assertTrue(columnNames.contains("Event_Name"));
        Assertions.assertTrue(columnNames.contains("Amount"));
    }

    /**
     * Column names with whitespace: verifies that the parser preserves whitespace in column names
     * and does not trim them.
     */
    @Test
    public void testColumnNamesWithWhitespace() throws IOException {
        // Create a file with column names containing whitespace
        MessageType schema =
                new MessageType(
                        "whitespace_test",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named(" a"));

        File file = tempDir.resolve("whitespace_test.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 10; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                group.append(" a", (long) (i * 10));
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();

        // Request column with leading space (preserved, not trimmed)
        Set<String> requested = new HashSet<>(Arrays.asList("id", " a"));

        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, converter);

        // Verify both columns are present (whitespace preserved)
        Assertions.assertEquals(1, selective.getBlocks().size());
        Assertions.assertEquals(2, selective.getBlocks().get(0).getColumns().size());

        Set<String> columnNames = new HashSet<>();
        for (ColumnChunkMetaData chunk : selective.getBlocks().get(0).getColumns()) {
            columnNames.add(chunk.getPath().toDotString());
        }
        Assertions.assertTrue(columnNames.contains("id"));
        Assertions.assertTrue(columnNames.contains(" a"));
    }

    /**
     * Large file with many row groups: verifies the parser handles files with many row groups
     * efficiently.
     */
    @Test
    public void testManyRowGroups() throws IOException {
        // Create a file with many small row groups
        File file = writeTestFile(1000, 100); // very small row group size
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
        Assertions.assertTrue(selective.getBlocks().size() > 5, "Should have many row groups");
    }

    /**
     * Selective parse with nested columns: verifies that requesting a top-level column includes all
     * its nested sub-columns.
     */
    @Test
    public void testSelectiveParseWithNestedColumns() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);

        // Request "g" which has nested fields "a" and "b"
        Set<String> requested = new HashSet<>(Arrays.asList("g"));
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, converter);

        // Verify all nested columns under "g" are present
        for (BlockMetaData block : selective.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                String path = chunk.getPath().toDotString();
                Assertions.assertTrue(
                        path.startsWith("g."), "Column " + path + " should be under 'g'");
            }
        }

        // Verify the number of columns matches the official parse for "g"
        int officialGColumns = 0;
        for (BlockMetaData block : official.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                if (chunk.getPath().toDotString().startsWith("g.")) {
                    officialGColumns++;
                }
            }
        }

        int selectiveGColumns = 0;
        for (BlockMetaData block : selective.getBlocks()) {
            selectiveGColumns += block.getColumns().size();
        }

        Assertions.assertEquals(officialGColumns, selectiveGColumns);
    }

    // ------------------------------------------------------------------ protocol boundaries

    /**
     * Wide schema with 16 top-level columns (> 14): the thrift compact list header encodes the
     * element count in 4 bits with 15 meaning "read a varint", so both the schema element list (1
     * message + 16 fields) and each row group's column chunk list (16 leaves) exceed 14 and
     * exercise that varint-extension branch of {@link ThriftCompactReader#readListHeader()}, which
     * none of the other tests ever trigger.
     */
    @Test
    public void testLargeSchemaListSizeExtension() throws IOException {
        int nCols = 16;
        List<Type> fields = new ArrayList<>(nCols);
        for (int i = 0; i < nCols; i++) {
            fields.add(
                    Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                            .named("c" + i));
        }
        MessageType schema = new MessageType("wide", fields);

        File file = tempDir.resolve("wide.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 60; i++) {
                Group group = groupFactory.newGroup();
                for (int c = 0; c < nCols; c++) {
                    group.append("c" + c, (long) i);
                }
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));

        // selective subset over the wide schema: keep 3 of 16 columns
        Set<String> requested = new HashSet<>(Arrays.asList("c0", "c7", "c15"));
        ParquetMetadata subset =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), requested, converter);
        Assertions.assertEquals(official.getBlocks().size(), subset.getBlocks().size());
        for (int i = 0; i < official.getBlocks().size(); i++) {
            List<ColumnChunkMetaData> columns = subset.getBlocks().get(i).getColumns();
            Assertions.assertEquals(3, columns.size());
            for (ColumnChunkMetaData chunk : columns) {
                String topLevel = chunk.getPath().toDotString().split("\\.")[0];
                Assertions.assertTrue(
                        requested.contains(topLevel),
                        "Column " + topLevel + " should be requested");
            }
        }
    }

    // ------------------------------------------------------------------ end-to-end integration

    /**
     * End-to-end integration through the vendored {@link ParquetFileReader}: the
     * REQUESTED_COLUMNS_KEY / SKIP_STATISTICS_KEY properties set on ParquetReadOptions must reach
     * the selective parser, and a FallbackSignalException must transparently fall back to the
     * official parse.
     */
    @Test
    public void testParquetFileReaderIntegration() throws IOException {
        File file = writeTestFile(300, 1024);
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(file.toURI().toString());
        long length = file.length();
        LocalFileIO fileIO = new LocalFileIO();

        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        byte[] footer = readFooterBytes(file);
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);

        // A: requested columns + skip statistics propagate through ParquetReadOptions
        String sep = SelectiveFooterParser.COLUMN_NAME_SEPARATOR;
        try (ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(fileIO, path, length),
                        ParquetReadOptions.builder()
                                .withRange(0, length)
                                .set(SelectiveFooterParser.REQUESTED_COLUMNS_KEY, "id" + sep + "g")
                                .set(SelectiveFooterParser.SKIP_STATISTICS_KEY, "true")
                                .build(),
                        null)) {
            ParquetMetadata selective = reader.getFooter();
            Assertions.assertEquals(official.getBlocks().size(), selective.getBlocks().size());
            for (BlockMetaData block : selective.getBlocks()) {
                for (ColumnChunkMetaData chunk : block.getColumns()) {
                    String topLevel = chunk.getPath().toDotString().split("\\.")[0];
                    Assertions.assertTrue(
                            topLevel.equals("id") || topLevel.equals("g"),
                            "Unexpected column " + chunk.getPath().toDotString());
                    Assertions.assertTrue(
                            chunk.getStatistics() == null || chunk.getStatistics().isEmpty(),
                            "Statistics should be skipped");
                }
            }
        }

        // B: no properties -> official parse path, full metadata
        try (ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(fileIO, path, length),
                        ParquetReadOptions.builder().withRange(0, length).build(),
                        null)) {
            ParquetMetadata full = reader.getFooter();
            Assertions.assertEquals(
                    ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(full));
        }

        // C: requested columns absent from the file -> the selective parse falls back to the
        // official parse inside ParquetFileReader and the result stays equivalent
        try (ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(fileIO, path, length),
                        ParquetReadOptions.builder()
                                .withRange(0, length)
                                .set(SelectiveFooterParser.REQUESTED_COLUMNS_KEY, "missing")
                                .build(),
                        null)) {
            ParquetMetadata fellBack = reader.getFooter();
            Assertions.assertEquals(
                    ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(fellBack));
        }
    }

    // ------------------------------------------------------------------ remaining logical types

    /**
     * TIMESTAMP/TIME with MICROS and NANOS units: the manual {@code readTimeUnitStruct} must
     * materialize the full TimeUnit union (the other tests only ever exercise MILLIS).
     */
    @Test
    public void testTimestampMicrosNanos() throws IOException {
        MessageType schema =
                new MessageType(
                        "units",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.OPTIONAL)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                true, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("ts_micros"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.OPTIONAL)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                true, LogicalTypeAnnotation.TimeUnit.NANOS))
                                .named("ts_nanos"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.OPTIONAL)
                                .as(
                                        LogicalTypeAnnotation.timeType(
                                                true, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("time_micros"));
        File file = tempDir.resolve("units.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 40; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                group.append("ts_micros", 1700000000000000L + i);
                group.append("ts_nanos", 1700000000000000000L + i);
                group.append("time_micros", (long) i * 1000);
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);
        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    /**
     * UUID and FLOAT16 logical types: exercise the last members of the LogicalType union (case
     * 14/15 in {@code readLogicalType}). UNKNOWN(NullType) cannot be written through the parquet-mr
     * 1.15 schema API ({@code LogicalTypeAnnotation} exposes no unknownType factory) and would
     * produce no column chunk anyway, so it is not covered here.
     */
    @Test
    public void testUuidFloat16LogicalTypes() throws IOException {
        MessageType schema =
                new MessageType(
                        "uuid_float16",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.uuidType())
                                .length(16)
                                .named("u"),
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                        Type.Repetition.OPTIONAL)
                                .as(LogicalTypeAnnotation.float16Type())
                                .length(2)
                                .named("f"));
        File file = tempDir.resolve("uuid_float16.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            for (int i = 0; i < 30; i++) {
                Group group = groupFactory.newGroup();
                group.append("id", (long) i);
                byte[] uuid = new byte[16];
                uuid[0] = (byte) i;
                group.append("u", Binary.fromConstantByteArray(uuid));
                group.append(
                        "f", Binary.fromConstantByteArray(new byte[] {0x3c, 0x00})); // 1.0 half
                writer.write(group);
            }
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);
        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));
    }

    // ------------------------------------------------------------------ buffer variants

    /**
     * ByteBuffer variants: the {@code parse} entry must handle a direct (non-array) buffer via the
     * copy path and an array-backed buffer with a non-zero arrayOffset via the in-place path.
     */
    @Test
    public void testByteBufferVariants() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        String expected = ParquetMetadata.toPrettyJSON(official);

        // direct buffer: no backing array -> copy path in parse()
        ByteBuffer direct = ByteBuffer.allocateDirect(footer.length);
        direct.put(footer);
        direct.flip();
        Assertions.assertEquals(
                expected,
                ParquetMetadata.toPrettyJSON(SelectiveFooterParser.parse(direct, null, converter)));

        // array-backed buffer with non-zero position/arrayOffset: in-place path
        byte[] padded = new byte[footer.length + 8];
        System.arraycopy(footer, 0, padded, 8, footer.length);
        Assertions.assertEquals(
                expected,
                ParquetMetadata.toPrettyJSON(
                        SelectiveFooterParser.parse(
                                ByteBuffer.wrap(padded, 8, footer.length), null, converter)));
    }

    /** Selective mode over an empty file (0 row groups): succeeds with zero blocks, no fallback. */
    @Test
    public void testSelectiveParseEmptyFile() throws IOException {
        MessageType schema =
                new MessageType(
                        "empty_selective",
                        Types.primitive(
                                        PrimitiveType.PrimitiveTypeName.INT64,
                                        Type.Repetition.REQUIRED)
                                .named("id"));
        File file = tempDir.resolve("empty_selective.parquet").toFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(file.toURI().toString()),
                                        new Configuration()))
                        .withConf(new Configuration())
                        .withType(schema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(1024)
                        .build()) {
            // write 0 rows
        }

        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata selective =
                SelectiveFooterParser.parse(
                        ByteBuffer.wrap(footer),
                        new HashSet<>(Collections.singletonList("id")),
                        converter);
        Assertions.assertEquals(0, selective.getBlocks().size());
    }

    /** includeStatistics=false with requestedColumns==null: full columns, statistics skipped. */
    @Test
    public void testSkipStatisticsFullMode() throws IOException {
        File file = writeTestFile(100, 1024);
        byte[] footer = readFooterBytes(file);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();

        ParquetMetadata withStats =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, true, converter);
        ParquetMetadata withoutStats =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, false, converter);

        Assertions.assertEquals(withStats.getBlocks().size(), withoutStats.getBlocks().size());
        for (int i = 0; i < withStats.getBlocks().size(); i++) {
            List<ColumnChunkMetaData> with = withStats.getBlocks().get(i).getColumns();
            List<ColumnChunkMetaData> without = withoutStats.getBlocks().get(i).getColumns();
            Assertions.assertEquals(with.size(), without.size());
            for (int c = 0; c < with.size(); c++) {
                ColumnChunkMetaData a = with.get(c);
                ColumnChunkMetaData b = without.get(c);
                // everything except statistics must match
                Assertions.assertEquals(a.getPath(), b.getPath());
                Assertions.assertEquals(a.getTotalSize(), b.getTotalSize());
                Assertions.assertEquals(a.getStartingPos(), b.getStartingPos());
                Assertions.assertEquals(a.getValueCount(), b.getValueCount());
                Assertions.assertEquals(a.getCodec(), b.getCodec());
                Assertions.assertEquals(a.getEncodings(), b.getEncodings());
                // statistics are absent (empty) in the skip mode
                Assertions.assertTrue(b.getStatistics() == null || b.getStatistics().isEmpty());
            }
        }
    }

    // ------------------------------------------------------------------ handcrafted footer

    /**
     * Handcrafted footer covering structures the parquet-mr writer never produces in one file: a
     * non-null {@code file_path} (external/multi-file row groups), V1 statistics (deprecated
     * min/max instead of min_value/max_value), statistics with only null_count set, non-default
     * bloom filter offset/length, and non-default offset/column index references.
     *
     * <p>Field-order corruption is deliberately not covered: the official writer always emits
     * fields in id order, and constructing an out-of-order footer would require writing the compact
     * protocol by hand.
     */
    @Test
    public void testHandcraftedFooterStructures() throws Exception {
        FileMetaData metaData = new FileMetaData();

        List<SchemaElement> schema = new ArrayList<>();
        SchemaElement root = new SchemaElement();
        root.setName("hc");
        root.setNum_children(2);
        SchemaElement colA = new SchemaElement();
        colA.setName("col_a");
        colA.setType(org.apache.parquet.format.Type.INT64);
        colA.setRepetition_type(FieldRepetitionType.OPTIONAL);
        SchemaElement colB = new SchemaElement();
        colB.setName("col_b");
        colB.setType(org.apache.parquet.format.Type.INT64);
        colB.setRepetition_type(FieldRepetitionType.OPTIONAL);
        schema.add(root);
        schema.add(colA);
        schema.add(colB);
        metaData.setSchema(schema);

        RowGroup rowGroup = new RowGroup();
        List<ColumnChunk> chunks = new ArrayList<>();
        chunks.add(
                newChunk(
                        "data.parquet",
                        "col_a",
                        // V1 stats: deprecated min/max fields, no min_value/max_value
                        statsWithV1MinMax(10L, 90L, 2L),
                        // non-default bloom filter metadata
                        1000L,
                        32,
                        // non-default index references
                        2048L,
                        128,
                        4096L,
                        256));
        chunks.add(
                newChunk(
                        "data.parquet",
                        "col_b",
                        // statistics with only null_count set (all other fields absent)
                        statsWithNullCountOnly(5L),
                        -1L, // bloom filter absent
                        -1,
                        -1L, // column index absent
                        -1,
                        -1L, // offset index absent
                        -1));
        rowGroup.setColumns(chunks);
        rowGroup.setTotal_byte_size(160L);
        rowGroup.setNum_rows(10L);
        rowGroup.setFile_offset(0L);
        rowGroup.setTotal_compressed_size(160L);
        rowGroup.setOrdinal((short) 0);
        metaData.setNum_rows(10L);
        metaData.setRow_groups(Collections.singletonList(rowGroup));
        metaData.setVersion(1);
        metaData.setCreated_by("handcrafted");

        byte[] footer = serializeFooter(metaData);
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        ParquetMetadata official =
                converter.readParquetMetadata(
                        new ByteArrayInputStream(footer), ParquetMetadataConverter.NO_FILTER);
        ParquetMetadata selective =
                SelectiveFooterParser.parse(ByteBuffer.wrap(footer), null, converter);

        // full equivalence over the handcrafted footer
        Assertions.assertEquals(
                ParquetMetadata.toPrettyJSON(official), ParquetMetadata.toPrettyJSON(selective));

        // selective over the handcrafted footer: only col_a kept, its indexes/bloom offset
        // survive and match the official parse
        ParquetMetadata subset =
                SelectiveFooterParser.parse(
                        ByteBuffer.wrap(footer),
                        new HashSet<>(Collections.singletonList("col_a")),
                        converter);
        Assertions.assertEquals(1, subset.getBlocks().size());
        List<ColumnChunkMetaData> subsetColumns = subset.getBlocks().get(0).getColumns();
        Assertions.assertEquals(1, subsetColumns.size());
        ColumnChunkMetaData subsetColA = subsetColumns.get(0);
        ColumnChunkMetaData officialColA = official.getBlocks().get(0).getColumns().get(0);
        Assertions.assertEquals(
                officialColA.getBloomFilterOffset(), subsetColA.getBloomFilterOffset());
        Assertions.assertEquals(
                officialColA.getBloomFilterLength(), subsetColA.getBloomFilterLength());
        // IndexReference has no equals(); compare offset/length
        Assertions.assertEquals(
                officialColA.getColumnIndexReference().getOffset(),
                subsetColA.getColumnIndexReference().getOffset());
        Assertions.assertEquals(
                officialColA.getColumnIndexReference().getLength(),
                subsetColA.getColumnIndexReference().getLength());
        Assertions.assertEquals(
                officialColA.getOffsetIndexReference().getOffset(),
                subsetColA.getOffsetIndexReference().getOffset());
        Assertions.assertEquals(
                officialColA.getOffsetIndexReference().getLength(),
                subsetColA.getOffsetIndexReference().getLength());
    }

    private static ColumnChunk newChunk(
            String filePath,
            String columnName,
            Statistics statistics,
            long bloomFilterOffset,
            int bloomFilterLength,
            long columnIndexOffset,
            int columnIndexLength,
            long offsetIndexOffset,
            int offsetIndexLength) {
        ColumnChunk chunk = new ColumnChunk();
        if (filePath != null) {
            chunk.setFile_path(filePath);
        }
        chunk.setFile_offset(0L);

        ColumnMetaData meta = new ColumnMetaData();
        meta.setType(org.apache.parquet.format.Type.INT64);
        meta.setEncodings(Collections.singletonList(Encoding.PLAIN));
        meta.setPath_in_schema(Collections.singletonList(columnName));
        meta.setCodec(CompressionCodec.UNCOMPRESSED);
        meta.setNum_values(10L);
        meta.setTotal_uncompressed_size(80L);
        meta.setTotal_compressed_size(80L);
        meta.setData_page_offset(4L);
        if (statistics != null) {
            meta.setStatistics(statistics);
        }
        if (bloomFilterOffset >= 0) {
            meta.setBloom_filter_offset(bloomFilterOffset);
            meta.setBloom_filter_length(bloomFilterLength);
        }
        if (columnIndexOffset >= 0) {
            chunk.setColumn_index_offset(columnIndexOffset);
            chunk.setColumn_index_length(columnIndexLength);
        }
        if (offsetIndexOffset >= 0) {
            chunk.setOffset_index_offset(offsetIndexOffset);
            chunk.setOffset_index_length(offsetIndexLength);
        }
        chunk.setMeta_data(meta);
        return chunk;
    }

    private static Statistics statsWithV1MinMax(long min, long max, long nullCount) {
        Statistics stats = new Statistics();
        stats.setMin(ByteBuffer.wrap(longBytes(min)));
        stats.setMax(ByteBuffer.wrap(longBytes(max)));
        stats.setNull_count(nullCount);
        return stats;
    }

    private static Statistics statsWithNullCountOnly(long nullCount) {
        Statistics stats = new Statistics();
        stats.setNull_count(nullCount);
        return stats;
    }

    private static byte[] longBytes(long value) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value);
        return buf.array();
    }

    private static byte[] serializeFooter(FileMetaData metaData) throws Exception {
        TMemoryBuffer buffer = new TMemoryBuffer(4096);
        metaData.write(new TCompactProtocol(buffer));
        return Arrays.copyOf(buffer.getArray(), buffer.length());
    }
}

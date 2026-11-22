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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests external Avro string-map layouts and subsequent writes with Paimon's schema. */
class AvroStringMapReadTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({
        "required, true, true",
        "required, false, false",
        "null-first, true, true",
        "null-second, true, true",
        "required, false, true",
        "null-first, true, false"
    })
    void testReadAndRewriteStringMaps(
            String layout, boolean nullableReadType, boolean nullableWriteType) throws Exception {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema nullSchema = Schema.create(Schema.Type.NULL);
        Schema valueSchema =
                layout.equals("required")
                        ? stringSchema
                        : Schema.createUnion(
                                layout.equals("null-first")
                                        ? Arrays.asList(nullSchema, stringSchema)
                                        : Arrays.asList(stringSchema, nullSchema));
        Schema schema =
                SchemaBuilder.record("record")
                        .fields()
                        .requiredInt("id")
                        .name("attrs")
                        .type(Schema.createMap(valueSchema))
                        .noDefault()
                        .requiredString("tail")
                        .endRecord();
        Map<String, String> firstMap = new LinkedHashMap<>();
        firstMap.put("a", "green");
        firstMap.put("b", layout.equals("required") || !nullableWriteType ? "yellow" : null);
        firstMap.put("c", "red");
        List<Map<String, String>> expected =
                Arrays.asList(
                        firstMap, Collections.emptyMap(), Collections.singletonMap("last", "done"));
        LocalFileIO fileIO = LocalFileIO.create();
        Path source = new Path(tempDir.resolve("external.avro").toUri());
        try (PositionOutputStream out = fileIO.newOutputStream(source, false);
                DataFileWriter<GenericRecord> writer =
                        new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, out);
            for (int i = 0; i < expected.size(); i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", i);
                record.put("attrs", expected.get(i));
                record.put("tail", "tail-" + i);
                writer.append(record);
            }
        }
        RowType rowType = rowType(nullableReadType);
        AvroFileFormat format = new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));
        Path rewritten = new Path(tempDir.resolve("rewritten.avro").toUri());
        List<Map<String, String>> actual = new ArrayList<>();
        try (PositionOutputStream out = fileIO.newOutputStream(rewritten, false);
                FormatWriter writer =
                        format.createWriterFactory(rowType(nullableWriteType)).create(out, "null");
                CloseableIterator<InternalRow> reader =
                        format.createReaderFactory(rowType, rowType, Collections.emptyList())
                                .createReader(
                                        new FormatReaderContext(
                                                fileIO,
                                                source,
                                                fileIO.getFileSize(source),
                                                null,
                                                null))
                                .toCloseableIterator()) {
            while (reader.hasNext()) {
                InternalRow row = reader.next();
                assertThat(row.getInt(0)).isEqualTo(actual.size());
                assertThat(row.getString(2).toString()).isEqualTo("tail-" + actual.size());
                InternalMap map = row.getMap(1);
                InternalArray keys = map.keyArray();
                InternalArray values = map.valueArray();
                Map<String, String> copied = new LinkedHashMap<>();
                for (int i = 0; i < map.size(); i++) {
                    copied.put(
                            keys.getString(i).toString(),
                            values.isNullAt(i) ? null : values.getString(i).toString());
                }
                actual.add(copied);
                // Pass the original row through, exercising the encoded-map writer fast path.
                writer.addElement(row);
            }
        }
        assertThat(actual).containsExactlyElementsOf(expected);
        List<Map<String, String>> rewrittenMaps = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader =
                new DataFileReader<>(new File(rewritten.toUri()), new GenericDatumReader<>())) {
            while (reader.hasNext()) {
                GenericRecord row = reader.next();
                assertThat(row.get("id")).isEqualTo(rewrittenMaps.size());
                assertThat(row.get("tail").toString()).isEqualTo("tail-" + rewrittenMaps.size());
                Map<?, ?> map = (Map<?, ?>) row.get("attrs");
                Map<String, String> copied = new LinkedHashMap<>();
                map.forEach(
                        (key, value) ->
                                copied.put(
                                        key.toString(), value == null ? null : value.toString()));
                rewrittenMaps.add(copied);
            }
        }
        assertThat(rewrittenMaps).containsExactlyElementsOf(expected);
    }

    @Test
    void testRejectNullWhenRewritingToRequiredValues() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        AvroFileFormat format = new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));
        RowType nullable = rowType(true);
        Path source = new Path(tempDir.resolve("nullable.avro").toUri());
        try (PositionOutputStream out = fileIO.newOutputStream(source, false);
                FormatWriter writer = format.createWriterFactory(nullable).create(out, "null")) {
            writer.addElement(
                    GenericRow.of(
                            1,
                            new GenericMap(
                                    Collections.singletonMap(BinaryString.fromString("key"), null)),
                            BinaryString.fromString("tail")));
        }

        Path target = new Path(tempDir.resolve("required.avro").toUri());
        try (PositionOutputStream out = fileIO.newOutputStream(target, false);
                FormatWriter writer =
                        format.createWriterFactory(rowType(false)).create(out, "null");
                CloseableIterator<InternalRow> reader =
                        format.createReaderFactory(nullable, nullable, Collections.emptyList())
                                .createReader(
                                        new FormatReaderContext(
                                                fileIO,
                                                source,
                                                fileIO.getFileSize(source),
                                                null,
                                                null))
                                .toCloseableIterator()) {
            InternalRow row = reader.next();
            assertThat(row.getMap(1)).isInstanceOf(AvroBytesStringMap.class);
            assertThatThrownBy(() -> writer.addElement(row))
                    .hasStackTraceContaining("Map value expected not null but found null value");
        }
    }

    private static RowType rowType(boolean nullableValues) {
        return RowType.builder()
                .field("id", DataTypes.INT())
                .field(
                        "attrs",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING().copy(nullableValues)))
                .field("tail", DataTypes.STRING())
                .build();
    }
}

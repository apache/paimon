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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.OrcOptions;
import org.apache.paimon.format.SupportsReaderFieldMetadata;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** An orc {@link FormatReadWriteTest}. */
public class OrcFormatReadWriteTest extends FormatReadWriteTest {

    private final FileFormat legacyFormat =
            new OrcFileFormat(
                    new FileFormatFactory.FormatContext(
                            new Options(
                                    new HashMap<String, String>() {
                                        {
                                            put(
                                                    OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE.key(),
                                                    "true");
                                        }
                                    }),
                            1024,
                            1024));

    private final FileFormat newFormat =
            new OrcFileFormat(
                    new FileFormatFactory.FormatContext(
                            new Options(
                                    new HashMap<String, String>() {
                                        {
                                            put(
                                                    OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE.key(),
                                                    "false");
                                        }
                                    }),
                            1024,
                            1024));

    protected OrcFormatReadWriteTest() {
        super("orc");
    }

    @Test
    public void testWriteMetadata() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()));

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = newFormat.createWriterFactory(rowType).create(out, "zstd");
        Map<String, String> fieldMetadata = new HashMap<>();
        fieldMetadata.put("paimon.test.field-key", "field-value");
        fieldMetadata.put("paimon.test.field-version", "1");
        Map<String, Map<String, String>> fieldMetadataByName = new HashMap<>();
        fieldMetadataByName.put("name", fieldMetadata);
        byte[] arrowSchemaBytes =
                FormatMetadataUtils.buildArrowSchemaMetadata(rowType, fieldMetadataByName, false);
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("paimon.test.key", "paimon-test-value".getBytes(StandardCharsets.UTF_8));
        metadata.put(FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY, arrowSchemaBytes);
        ((SupportsWriterMetadata) writer).addMetadata(metadata);
        writer.addElement(GenericRow.of(1, org.apache.paimon.data.BinaryString.fromString("one")));
        writer.close();
        assertThatThrownBy(() -> ((SupportsWriterMetadata) writer).addMetadata(metadata))
                .isInstanceOf(IllegalStateException.class);
        out.close();

        try (org.apache.orc.Reader reader =
                OrcReaderFactory.createReader(
                        new org.apache.hadoop.conf.Configuration(false), fileIO, file, null)) {
            ByteBuffer value = reader.getMetadataValue("paimon.test.key");
            Map<String, byte[]> decodedMetadata =
                    FormatMetadataUtils.decodeMetadata(
                            Collections.singletonMap(
                                    "paimon.test.key",
                                    StandardCharsets.UTF_8.decode(value.duplicate()).toString()));
            assertThat(new String(decodedMetadata.get("paimon.test.key"), StandardCharsets.UTF_8))
                    .isEqualTo("paimon-test-value");
        }

        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        RowType emptyRowType = new RowType(Collections.emptyList());
        try (FileRecordReader<InternalRow> reader =
                newFormat
                        .createReaderFactory(emptyRowType, emptyRowType, Collections.emptyList())
                        .createReader(context)) {
            Map<String, Map<String, String>> readFieldMetadata =
                    ((SupportsReaderFieldMetadata) reader).readFieldMetadata();
            assertThat(readFieldMetadata).containsOnlyKeys("name");
            assertThat(readFieldMetadata.get("name")).containsAllEntriesOf(fieldMetadata);
        }
    }

    @Test
    public void testTimestampLTZWithLegacyWriteAndRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = legacyFormat.createWriterFactory(rowType).create(out, "zstd");
        Timestamp localTimestamp =
                Timestamp.fromLocalDateTime(
                        LocalDateTime.parse(
                                "2024-12-12 10:10:10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        GenericRow record = GenericRow.of(localTimestamp);
        writer.addElement(record);
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                legacyFormat
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).containsExactly(GenericRow.of(localTimestamp));
    }

    @Test
    public void testTimestampLTZWithNewWriteAndRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = newFormat.createWriterFactory(rowType).create(out, "zstd");
        Timestamp localTimestamp =
                Timestamp.fromLocalDateTime(
                        LocalDateTime.parse(
                                "2024-12-12 10:10:10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        GenericRow record = GenericRow.of(localTimestamp);
        writer.addElement(record);
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                newFormat
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).containsExactly(GenericRow.of(localTimestamp));
    }

    @Test
    public void testTimestampLTZWithNewWriteAndLegacyRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = newFormat.createWriterFactory(rowType).create(out, "zstd");
        Timestamp localTimestamp =
                Timestamp.fromLocalDateTime(
                        LocalDateTime.parse(
                                "2024-12-12 10:10:10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        GenericRow record = GenericRow.of(localTimestamp);
        writer.addElement(record);
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                legacyFormat
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        Timestamp shiftedTimestamp =
                Timestamp.fromEpochMillis(
                        localTimestamp.getMillisecond()
                                + TimeZone.getDefault().getOffset(localTimestamp.getMillisecond()),
                        localTimestamp.getNanoOfMillisecond());

        assertThat(result).containsExactly(GenericRow.of(shiftedTimestamp));
    }

    @Test
    public void testTimestampLTZWithLegacyWriteAndNewRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = legacyFormat.createWriterFactory(rowType).create(out, "zstd");
        Timestamp localTimestamp =
                Timestamp.fromLocalDateTime(
                        LocalDateTime.parse(
                                "2024-12-12 10:10:10",
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        GenericRow record = GenericRow.of(localTimestamp);
        writer.addElement(record);
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                newFormat
                        .createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        Timestamp shiftedTimestamp =
                Timestamp.fromEpochMillis(
                        localTimestamp.getMillisecond()
                                - TimeZone.getDefault().getOffset(localTimestamp.getMillisecond()),
                        localTimestamp.getNanoOfMillisecond());

        assertThat(result).containsExactly(GenericRow.of(shiftedTimestamp));
    }

    @Override
    protected FileFormat fileFormat() {
        return new OrcFileFormat(
                new FileFormatFactory.FormatContext(
                        new Options(
                                new HashMap<String, String>() {
                                    {
                                        put(
                                                OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE.key(),
                                                "false");
                                    }
                                }),
                        1024,
                        1024));
    }
}

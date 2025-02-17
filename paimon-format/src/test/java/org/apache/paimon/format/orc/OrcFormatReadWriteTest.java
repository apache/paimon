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
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.OrcOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

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
                        .createReaderFactory(rowType)
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
                        .createReaderFactory(rowType)
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
                        .createReaderFactory(rowType)
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
                        .createReaderFactory(rowType)
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

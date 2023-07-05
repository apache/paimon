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

package org.apache.paimon.format;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test Base class for Format. */
public abstract class FormatReadWriteTest {

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private Path file;

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    protected abstract FileFormat fileFormat();

    @Test
    public void testSimpleTypes() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.BIGINT());

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = (RowType) rowType.notNull();
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        FileFormat format = fileFormat();

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
        writer.addElement(GenericRow.of(1, 1L));
        writer.addElement(GenericRow.of(2, 2L));
        writer.addElement(GenericRow.of(3, null));
        writer.flush();
        writer.finish();
        out.close();

        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType).createReader(fileIO, file);
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result)
                .containsExactly(
                        GenericRow.of(1, 1L), GenericRow.of(2, 2L), GenericRow.of(3, null));
    }

    @Test
    public void testFullTypes() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("name", DataTypes.STRING()) /* optional by default */
                        .field("salary", DataTypes.DOUBLE().notNull())
                        .field(
                                "locations",
                                DataTypes.MAP(
                                        DataTypes.STRING().notNull(),
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        0,
                                                        "posX",
                                                        DataTypes.DOUBLE().notNull(),
                                                        "X field"),
                                                DataTypes.FIELD(
                                                        1,
                                                        "posY",
                                                        DataTypes.DOUBLE().notNull(),
                                                        "Y field"))))
                        .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
                        .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .field("boolean", DataTypes.BOOLEAN().nullable())
                        .field("tinyint", DataTypes.TINYINT())
                        .field("smallint", DataTypes.SMALLINT())
                        .field("bigint", DataTypes.BIGINT())
                        .field("bytes", DataTypes.BYTES())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .field("timestamp_3", DataTypes.TIMESTAMP(3))
                        .field("date", DataTypes.DATE())
                        .field("decimal", DataTypes.DECIMAL(2, 2))
                        .field("decimal2", DataTypes.DECIMAL(38, 2))
                        .field("decimal3", DataTypes.DECIMAL(10, 1))
                        .build();

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = (RowType) rowType.notNull();
        }

        FileFormat format = fileFormat();

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
        GenericRow expected =
                GenericRow.of(
                        1,
                        fromString("name"),
                        5.26D,
                        new GenericMap(
                                new HashMap<Object, Object>() {
                                    {
                                        this.put(fromString("key1"), GenericRow.of(5.2D, 6.2D));
                                        this.put(fromString("key2"), GenericRow.of(6.2D, 2.2D));
                                    }
                                }),
                        new GenericArray(new Object[] {fromString("123"), fromString("456")}),
                        new GenericArray(new Object[] {123, 456}),
                        true,
                        (byte) 3,
                        (short) 6,
                        12304L,
                        new byte[] {1, 5, 2},
                        Timestamp.fromMicros(123123123),
                        Timestamp.fromEpochMillis(123123123),
                        2456,
                        Decimal.fromBigDecimal(new BigDecimal(0.22), 2, 2),
                        Decimal.fromBigDecimal(new BigDecimal(12312455.22), 38, 2),
                        Decimal.fromBigDecimal(new BigDecimal(12455.1), 10, 1));
        writer.addElement(expected);
        writer.flush();
        writer.finish();
        out.close();

        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType).createReader(fileIO, file);
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(result::add);

        assertThat(result).containsExactly(expected);
    }
}

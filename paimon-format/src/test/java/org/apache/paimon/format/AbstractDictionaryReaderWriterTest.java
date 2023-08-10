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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;

/** IT Case for the read/writer of format. */
public abstract class AbstractDictionaryReaderWriterTest {
    private @TempDir java.nio.file.Path tempDir;

    protected Path path;
    protected RowType rowType;

    @BeforeEach
    public void before() throws IOException {
        rowType =
                RowType.builder()
                        .field("a0", DataTypes.INT())
                        .field("a1", DataTypes.TINYINT())
                        .field("a2", DataTypes.SMALLINT())
                        .field("a3", DataTypes.BIGINT())
                        .field("a4", DataTypes.STRING())
                        .field("a5", DataTypes.DOUBLE())
                        .field("a6", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("a7", DataTypes.CHAR(100))
                        .field("a8", DataTypes.VARCHAR(100))
                        .field("a9", DataTypes.BOOLEAN())
                        .field("a10", DataTypes.DATE())
                        .field("a11", DataTypes.TIME())
                        .field("a12", DataTypes.TIMESTAMP())
                        .field("a13", DataTypes.TIMESTAMP_MILLIS())
                        .field("a14", DataTypes.DECIMAL(3, 3))
                        .field("a15", DataTypes.BYTES())
                        .field("a16", DataTypes.FLOAT())
                        .field("a17", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .field("a18", DataTypes.ROW(DataTypes.FIELD(100, "b1", DataTypes.STRING())))
                        .field("a19", DataTypes.BINARY(100))
                        .field("a20", DataTypes.VARBINARY(100))
                        .field("a21", DataTypes.MULTISET(DataTypes.STRING()))
                        .field("a22", DataTypes.STRING())
                        .field(
                                "a23",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                101,
                                                "b2",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                102,
                                                                "b3",
                                                                DataTypes.MAP(
                                                                        DataTypes.STRING(),
                                                                        DataTypes.STRING())),
                                                        DataTypes.FIELD(
                                                                103,
                                                                "b4",
                                                                DataTypes.ARRAY(
                                                                        DataTypes.STRING())),
                                                        DataTypes.FIELD(
                                                                104,
                                                                "b5",
                                                                DataTypes.MULTISET(
                                                                        DataTypes.STRING()))))))
                        .build();
        FormatWriterFactory writerFactory = getFileFormat().createWriterFactory(rowType);
        path = new Path(tempDir.toUri().toString(), "1." + getFileFormat().getFormatIdentifier());
        LocalFileIO localFileIO = LocalFileIO.create();
        PositionOutputStream out = localFileIO.newOutputStream(path, false);
        FormatWriter formatWriter = writerFactory.create(out, null);

        for (int i = 0; i < 1024; i++) {
            GenericRow row = new GenericRow(24);
            row.setField(0, 1);
            row.setField(1, (byte) 1);
            row.setField(2, (short) 1);
            row.setField(3, 1L);
            row.setField(4, BinaryString.fromString("a"));
            row.setField(5, 0.5D);
            row.setField(6, new GenericArray(new Object[] {BinaryString.fromString("1")}));
            row.setField(7, BinaryString.fromString("3"));
            row.setField(8, BinaryString.fromString("3"));
            row.setField(9, true);
            row.setField(10, 375);
            row.setField(11, 100);
            row.setField(12, Timestamp.fromEpochMillis(1685548953000L));
            row.setField(13, Timestamp.fromEpochMillis(1685548953000L));
            row.setField(14, Decimal.fromBigDecimal(new BigDecimal("0.22"), 3, 3));
            row.setField(15, new byte[] {1, 5, 2});
            row.setField(16, 0.26F);
            row.setField(
                    17,
                    new GenericMap(
                            Collections.singletonMap(
                                    BinaryString.fromString("k"), BinaryString.fromString("v"))));
            row.setField(18, GenericRow.of(BinaryString.fromString("cc")));
            row.setField(19, "bb".getBytes());
            row.setField(20, "aa".getBytes());
            row.setField(
                    21,
                    new GenericMap(Collections.singletonMap(BinaryString.fromString("set"), 1)));
            row.setField(22, BinaryString.fromString("a"));
            row.setField(
                    23,
                    GenericRow.of(
                            GenericRow.of(
                                    new GenericMap(
                                            Collections.singletonMap(
                                                    BinaryString.fromString("k"),
                                                    BinaryString.fromString("v"))),
                                    new GenericArray(new Object[] {BinaryString.fromString("1")}),
                                    new GenericMap(
                                            Collections.singletonMap(
                                                    BinaryString.fromString("set"), 1)))));

            formatWriter.addElement(row);
        }

        formatWriter.flush();
        formatWriter.finish();
        out.close();
    }

    public RowType getRowType() {
        return rowType;
    }

    protected abstract FileFormat getFileFormat();
}

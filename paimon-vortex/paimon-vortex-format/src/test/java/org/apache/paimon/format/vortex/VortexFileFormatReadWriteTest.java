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

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import dev.vortex.api.DType;
import org.junit.jupiter.api.BeforeAll;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test for Vortex file format read/write using the base test framework. */
public class VortexFileFormatReadWriteTest extends FormatReadWriteTest {

    protected VortexFileFormatReadWriteTest() {
        super("vortex");
    }

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Vortex native library not available, skipping tests");
    }

    private static boolean isNativeAvailable() {
        try {
            dev.vortex.jni.NativeLoader.loadJni();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static boolean isFixedSizeListSupported() {
        try {
            DType.newFixedSizeList(DType.newInt(false), 2, false);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public boolean supportNestedReadPruning() {
        return false;
    }

    @Override
    public String compression() {
        return "";
    }

    @Override
    protected FileFormat fileFormat() {
        return new VortexFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Override
    protected RowType rowTypeForFullTypesTest() {
        RowType.Builder builder =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("name", DataTypes.STRING())
                        .field("salary", DataTypes.DOUBLE().notNull())
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
                        .field(
                                "rowArray",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        0,
                                                        "int0",
                                                        DataTypes.INT().notNull(),
                                                        "nested row int field 0"),
                                                DataTypes.FIELD(
                                                        1,
                                                        "double1",
                                                        DataTypes.DOUBLE().notNull(),
                                                        "nested row double field 1"))))
                        .field("floatVector", DataTypes.VECTOR(3, DataTypes.FLOAT()));

        RowType rowType = builder.build();
        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = rowType.notNull();
        }
        return rowType;
    }

    @Override
    protected GenericRow expectedRowForFullTypesTest() {
        return GenericRow.of(
                1,
                fromString("name"),
                5.26D,
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
                Decimal.fromBigDecimal(new BigDecimal("0.22"), 2, 2),
                Decimal.fromBigDecimal(new BigDecimal("12312455.22"), 38, 2),
                Decimal.fromBigDecimal(new BigDecimal("12455.1"), 10, 1),
                new GenericArray(new Object[] {GenericRow.of(1, 0.1D), GenericRow.of(2, 0.2D)}),
                BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f}));
    }
}

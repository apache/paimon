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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link SequenceGenerator}. */
public class SequenceGeneratorTest {

    private static final RowType ALL_DATA_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), // _id
                        DataTypes.DECIMAL(2, 1), // pt
                        DataTypes.INT(), // second
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT(), // millis
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.STRING(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.CHAR(10),
                        DataTypes.VARCHAR(20),
                        DataTypes.BINARY(10),
                        DataTypes.VARBINARY(20),
                        DataTypes.BYTES(),
                        DataTypes.TIME(),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MULTISET(DataTypes.VARCHAR(8))
                    },
                    new String[] {
                        "_id",
                        "pt",
                        "_intsecond",
                        "_boolean",
                        "_tinyint",
                        "_smallint",
                        "_bigint",
                        "_bigintmillis",
                        "_float",
                        "_double",
                        "_string",
                        "_date",
                        "_timestamp0",
                        "_timestamp3",
                        "_timestamp6",
                        "_char",
                        "_varchar",
                        "_binary",
                        "_varbinary",
                        "_bytes",
                        "_time",
                        "_localtimestamp",
                        "_map",
                        "_array",
                        "_multiset",
                    });
    private static final InternalRow row =
            GenericRow.of(
                    1,
                    Decimal.fromUnscaledLong(10, 2, 1),
                    1685548953,
                    true,
                    (byte) 2,
                    (short) 3,
                    4000000000000L,
                    1685548953000L,
                    2.81f,
                    3.678008,
                    BinaryString.fromString("1"),
                    375, /* 1971-01-11 */
                    Timestamp.fromEpochMillis(1685548953000L),
                    Timestamp.fromEpochMillis(1685548953123L),
                    Timestamp.fromMicros(1685548953123456L),
                    BinaryString.fromString("3"),
                    BinaryString.fromString("4"),
                    "5".getBytes(),
                    "6".getBytes(),
                    "7".getBytes(),
                    123,
                    Timestamp.fromMicros(1685548953123456L),
                    new GenericMap(
                            Collections.singletonMap(
                                    BinaryString.fromString("mapKey"),
                                    BinaryString.fromString("mapVal"))),
                    new GenericArray(
                            new BinaryString[] {
                                BinaryString.fromString("a"), BinaryString.fromString("b")
                            }),
                    new GenericMap(
                            Collections.singletonMap(BinaryString.fromString("multiset"), 1)));

    @Test
    public void testGenerate() {
        assertEquals(1, getGenerator("_id").generate(row));
        assertEquals(1, getGenerator("pt").generate(row));
        assertEquals(1685548953, getGenerator("_intsecond").generate(row));
        assertEquals(2, getGenerator("_tinyint").generate(row));
        assertEquals(3, getGenerator("_smallint").generate(row));
        assertEquals(4000000000000L, getGenerator("_bigint").generate(row));
        assertEquals(1685548953000L, getGenerator("_bigintmillis").generate(row));
        assertEquals(2, getGenerator("_float").generate(row));
        assertEquals(3, getGenerator("_double").generate(row));
        assertEquals(1, getGenerator("_string").generate(row));
        assertEquals(375, getGenerator("_date").generate(row));
        assertEquals(1685548953000L, getGenerator("_timestamp0").generate(row));
        assertEquals(1685548953123L, getGenerator("_timestamp3").generate(row));
        assertEquals(1685548953123L, getGenerator("_timestamp6").generate(row));
        assertEquals(3, getGenerator("_char").generate(row));
        assertEquals(4, getGenerator("_varchar").generate(row));
        assertEquals(1685548953123L, getGenerator("_localtimestamp").generate(row));
        assertUnsupportedDatatype("_boolean");
        assertUnsupportedDatatype("_binary");
        assertUnsupportedDatatype("_varbinary");
        assertUnsupportedDatatype("_bytes");
        assertUnsupportedDatatype("_time");
        assertUnsupportedDatatype("_map");
        assertUnsupportedDatatype("_array");
        assertUnsupportedDatatype("_multiset");
    }

    @Test
    public void testGenerateWithPadding() {
        assertEquals(1, getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("_id")));
        assertEquals(1, getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("pt")));
        assertEquals(
                1685548953,
                getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("_intsecond")));
        assertEquals(2, getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("_tinyint")));
        assertEquals(
                3, getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("_smallint")));
        assertEquals(
                4000000000000L,
                getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_bigint")));
        assertEquals(
                1685548953000L,
                getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_bigintmillis")));
        assertEquals(2, getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_float")));
        assertEquals(3, getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_double")));
        assertEquals(1, getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_string")));
        assertEquals(375, getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_date")));
        assertEquals(
                1685548953L,
                getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("_timestamp0")));
        assertEquals(
                1685548953123L,
                getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_timestamp3")));
        assertEquals(
                1685548953123L,
                getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_timestamp6")));
        assertEquals(3, getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_char")));
        assertEquals(4, getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_varchar")));
        assertEquals(
                1685548953L,
                getSecondFromGeneratedWithPadding(generateWithPaddingOnSecond("_localtimestamp")));
        assertEquals(
                1685548953123L,
                getMillisFromGeneratedWithPadding(generateWithPaddingOnMillis("_localtimestamp")));
        assertUnsupportedDatatype("_boolean");
        assertUnsupportedDatatype("_binary");
        assertUnsupportedDatatype("_varbinary");
        assertUnsupportedDatatype("_bytes");
        assertUnsupportedDatatype("_time");
        assertUnsupportedDatatype("_map");
        assertUnsupportedDatatype("_array");
        assertUnsupportedDatatype("_multiset");
    }

    private SequenceGenerator getGenerator(String field) {
        return new SequenceGenerator(field, ALL_DATA_TYPE);
    }

    private void assertUnsupportedDatatype(String field) {
        assertThrows(UnsupportedOperationException.class, () -> getGenerator(field).generate(row));
    }

    private long generateWithPaddingOnSecond(String field) {
        return getGenerator(field)
                .generateWithPadding(row, CoreOptions.SequenceAutoPadding.SECOND_TO_MICRO);
    }

    private long getSecondFromGeneratedWithPadding(long generated) {
        return TimeUnit.SECONDS.convert(generated, TimeUnit.MICROSECONDS);
    }

    private long generateWithPaddingOnMillis(String field) {
        return getGenerator(field)
                .generateWithPadding(row, CoreOptions.SequenceAutoPadding.MILLIS_TO_MICRO);
    }

    private long getMillisFromGeneratedWithPadding(long generated) {
        return TimeUnit.MILLISECONDS.convert(generated, TimeUnit.MICROSECONDS);
    }
}

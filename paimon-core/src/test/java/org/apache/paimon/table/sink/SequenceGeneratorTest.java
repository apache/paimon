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
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.SequenceAutoPadding.ROW_KIND_FLAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(getGenerator("_id").generate(row)).isEqualTo(1);
        assertThat(getGenerator("pt").generate(row)).isEqualTo(1);
        assertThat(getGenerator("_intsecond").generate(row)).isEqualTo(1685548953);
        assertThat(getGenerator("_tinyint").generate(row)).isEqualTo(2);
        assertThat(getGenerator("_smallint").generate(row)).isEqualTo(3);
        assertThat(getGenerator("_bigint").generate(row)).isEqualTo(4000000000000L);
        assertThat(getGenerator("_bigintmillis").generate(row)).isEqualTo(1685548953000L);
        assertThat(getGenerator("_float").generate(row)).isEqualTo(2);
        assertThat(getGenerator("_double").generate(row)).isEqualTo(3);
        assertThat(getGenerator("_string").generate(row)).isEqualTo(1);
        assertThat(getGenerator("_date").generate(row)).isEqualTo(375);
        assertThat(getGenerator("_timestamp0").generate(row)).isEqualTo(1685548953000L);
        assertThat(getGenerator("_timestamp3").generate(row)).isEqualTo(1685548953123L);
        assertThat(getGenerator("_timestamp6").generate(row)).isEqualTo(1685548953123L);
        assertThat(getGenerator("_char").generate(row)).isEqualTo(3);
        assertThat(getGenerator("_varchar").generate(row)).isEqualTo(4);
        assertThat(getGenerator("_localtimestamp").generate(row)).isEqualTo(1685548953123L);
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
    public void testGenerateWithPaddingRowKind() {
        assertThat(generateWithPaddingOnRowKind(1L, RowKind.INSERT)).isEqualTo(3);
        assertThat(generateWithPaddingOnRowKind(1L, RowKind.UPDATE_AFTER)).isEqualTo(3);
        assertThat(generateWithPaddingOnRowKind(1L, RowKind.UPDATE_BEFORE)).isEqualTo(2);
        assertThat(generateWithPaddingOnRowKind(1L, RowKind.DELETE)).isEqualTo(2);

        long maxMicros =
                Timestamp.fromLocalDateTime(LocalDateTime.parse("5000-01-01T00:00:00")).toMicros();
        assertThat(generateWithPaddingOnRowKind(maxMicros, RowKind.INSERT))
                .isEqualTo(191235168000000001L);
    }

    private SequenceGenerator getGenerator(String field) {
        return getGenerator(field, Collections.emptyList());
    }

    private SequenceGenerator getGenerator(
            String field, List<CoreOptions.SequenceAutoPadding> paddings) {
        return new SequenceGenerator(field, ALL_DATA_TYPE, paddings);
    }

    private void assertUnsupportedDatatype(String field) {
        assertThatThrownBy(() -> getGenerator(field).generate(row))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private long generateWithPaddingOnRowKind(long sequence, RowKind rowKind) {
        return getGenerator("_bigint", Collections.singletonList(ROW_KIND_FLAG))
                .generate(GenericRow.ofKind(rowKind, 0, 0, 0, 0, 0, 0, sequence));
    }
}

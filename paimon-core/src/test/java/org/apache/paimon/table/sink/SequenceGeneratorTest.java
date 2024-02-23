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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.SequenceAutoPadding.MILLIS_TO_MICRO;
import static org.apache.paimon.CoreOptions.SequenceAutoPadding.ROW_KIND_FLAG;
import static org.apache.paimon.CoreOptions.SequenceAutoPadding.SECOND_TO_MICRO;
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
        assertThat(getGenerator("_id").generateWithPadding(row, 0)).isEqualTo(1);
        assertThat(getGenerator("pt").generateWithPadding(row, 0)).isEqualTo(1);
        assertThat(getGenerator("_intsecond").generateWithPadding(row, 0)).isEqualTo(1685548953);
        assertThat(getGenerator("_tinyint").generateWithPadding(row, 0)).isEqualTo(2);
        assertThat(getGenerator("_smallint").generateWithPadding(row, 0)).isEqualTo(3);
        assertThat(getGenerator("_bigint").generateWithPadding(row, 0)).isEqualTo(4000000000000L);
        assertThat(getGenerator("_bigintmillis").generateWithPadding(row, 0))
                .isEqualTo(1685548953000L);
        assertThat(getGenerator("_float").generateWithPadding(row, 0)).isEqualTo(2);
        assertThat(getGenerator("_double").generateWithPadding(row, 0)).isEqualTo(3);
        assertThat(getGenerator("_string").generateWithPadding(row, 0)).isEqualTo(1);
        assertThat(getGenerator("_date").generateWithPadding(row, 0)).isEqualTo(375);
        assertThat(getGenerator("_timestamp0").generateWithPadding(row, 0))
                .isEqualTo(1685548953000L);
        assertThat(getGenerator("_timestamp3").generateWithPadding(row, 0))
                .isEqualTo(1685548953123L);
        assertThat(getGenerator("_timestamp6").generateWithPadding(row, 0))
                .isEqualTo(1685548953123L);
        assertThat(getGenerator("_char").generateWithPadding(row, 0)).isEqualTo(3);
        assertThat(getGenerator("_varchar").generateWithPadding(row, 0)).isEqualTo(4);
        assertThat(getGenerator("_localtimestamp").generateWithPadding(row, 0))
                .isEqualTo(1685548953123L);
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
        assertThat(generateWithPaddingOnSecond("_id", 5)).isEqualTo(1000005L);
        assertThat(generateWithPaddingOnSecond("pt", 5)).isEqualTo(1000005L);

        assertThat(generateWithPaddingOnSecond("_intsecond", 5)).isEqualTo(1685548953000005L);
        assertThat(generateWithPaddingOnSecond("_tinyint", 5)).isEqualTo(2000005L);

        assertThat(generateWithPaddingOnSecond("_smallint", 5)).isEqualTo(3000005L);

        assertThat(generateWithPaddingOnMillis("_bigint", 5)).isEqualTo(4000000000000005L);

        assertThat(generateWithPaddingOnMillis("_bigintmillis", 5)).isEqualTo(1685548953000005L);

        assertThat(generateWithPaddingOnMillis("_float", 5)).isEqualTo(2005);

        assertThat(generateWithPaddingOnMillis("_double", 5)).isEqualTo(3005);

        assertThat(generateWithPaddingOnMillis("_string", 5)).isEqualTo(1005);

        assertThat(generateWithPaddingOnMillis("_date", 5)).isEqualTo(375005);

        assertThat(generateWithPaddingOnSecond("_timestamp0", 5)).isEqualTo(1685548953000005L);

        assertThat(generateWithPaddingOnMillis("_timestamp3", 5)).isEqualTo(1685548953123005L);

        assertThat(generateWithPaddingOnMillis("_timestamp6", 5)).isEqualTo(1685548953123005L);

        assertThat(generateWithPaddingOnMillis("_char", 5)).isEqualTo(3005);

        assertThat(generateWithPaddingOnMillis("_varchar", 5)).isEqualTo(4005);
        assertThat(generateWithPaddingOnSecond("_localtimestamp", 5)).isEqualTo(1685548953000005L);
        assertThat(generateWithPaddingOnMillis("_localtimestamp", 5)).isEqualTo(1685548953123005L);
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

        long sequence = generateWithPaddingOnMicrosAndRowKind(1L, 20, RowKind.INSERT);
        assertThat(sequence).isEqualTo(2041);
        sequence = generateWithPaddingOnMicrosAndRowKind(1L, 30, RowKind.UPDATE_BEFORE);
        System.out.println(sequence);
        assertThat(sequence).isEqualTo(2060);
    }

    private SequenceGenerator getGenerator(String field) {
        return getGenerator(field, Collections.emptyList());
    }

    private SequenceGenerator getGenerator(
            String field, List<CoreOptions.SequenceAutoPadding> paddings) {
        return new SequenceGenerator(field, ALL_DATA_TYPE, paddings);
    }

    private void assertUnsupportedDatatype(String field) {
        assertThatThrownBy(() -> getGenerator(field).generateWithPadding(row, 0))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private long generateWithPaddingOnSecond(String field, long incrSeq) {
        return getGenerator(field, Collections.singletonList(SECOND_TO_MICRO))
                .generateWithPadding(row, incrSeq);
    }

    private long generateWithPaddingOnMillis(String field, long incrSeq) {
        return getGenerator(field, Collections.singletonList(MILLIS_TO_MICRO))
                .generateWithPadding(row, incrSeq);
    }

    private long generateWithPaddingOnRowKind(long sequence, RowKind rowKind) {
        return getGenerator("_bigint", Collections.singletonList(ROW_KIND_FLAG))
                .generateWithPadding(GenericRow.ofKind(rowKind, 0, 0, 0, 0, 0, 0, sequence), 0);
    }

    private long generateWithPaddingOnMicrosAndRowKind(
            long sequence, long incrSeq, RowKind rowKind) {
        return getGenerator("_bigint", Arrays.asList(MILLIS_TO_MICRO, ROW_KIND_FLAG))
                .generateWithPadding(
                        GenericRow.ofKind(rowKind, 0, 0, 0, 0, 0, 0, sequence), incrSeq);
    }
}

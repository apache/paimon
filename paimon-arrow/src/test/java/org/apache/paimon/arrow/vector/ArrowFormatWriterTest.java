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

package org.apache.paimon.arrow.vector;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/** Test for {@link org.apache.paimon.arrow.vector.ArrowFormatWriter}. */
public class ArrowFormatWriterTest {

    private static final Random RND = ThreadLocalRandom.current();
    private static final boolean[] NULLABLE;
    private static final RowType PRIMITIVE_TYPE;

    static {
        int cnt = 18;
        NULLABLE = new boolean[cnt];
        for (int i = 0; i < cnt; i++) {
            NULLABLE[i] = RND.nextBoolean();
        }

        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "char", DataTypes.CHAR(10).copy(NULLABLE[0])));
        dataFields.add(new DataField(1, "varchar", DataTypes.VARCHAR(20).copy(NULLABLE[1])));
        dataFields.add(new DataField(2, "boolean", DataTypes.BOOLEAN().copy(NULLABLE[2])));
        dataFields.add(new DataField(3, "binary", DataTypes.BINARY(10).copy(NULLABLE[3])));
        dataFields.add(new DataField(4, "varbinary", DataTypes.VARBINARY(20).copy(NULLABLE[4])));
        dataFields.add(new DataField(5, "decimal1", DataTypes.DECIMAL(2, 2).copy(NULLABLE[5])));
        dataFields.add(new DataField(6, "decimal2", DataTypes.DECIMAL(38, 2).copy(NULLABLE[6])));
        dataFields.add(new DataField(7, "decimal3", DataTypes.DECIMAL(10, 1).copy(NULLABLE[7])));
        dataFields.add(new DataField(8, "tinyint", DataTypes.TINYINT().copy(NULLABLE[8])));
        dataFields.add(new DataField(9, "smallint", DataTypes.SMALLINT().copy(NULLABLE[9])));
        dataFields.add(new DataField(10, "int", DataTypes.INT().copy(NULLABLE[10])));
        dataFields.add(new DataField(11, "bigint", DataTypes.BIGINT().copy(NULLABLE[11])));
        dataFields.add(new DataField(12, "float", DataTypes.FLOAT().copy(NULLABLE[12])));
        dataFields.add(new DataField(13, "double", DataTypes.DOUBLE().copy(NULLABLE[13])));
        dataFields.add(new DataField(14, "date", DataTypes.DATE().copy(NULLABLE[14])));
        dataFields.add(new DataField(15, "timestamp3", DataTypes.TIMESTAMP(3).copy(NULLABLE[15])));
        dataFields.add(new DataField(16, "timestamp6", DataTypes.TIMESTAMP(6).copy(NULLABLE[16])));
        dataFields.add(
                new DataField(
                        17,
                        "timestampLZ9",
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9).copy(NULLABLE[17])));
        PRIMITIVE_TYPE = new RowType(dataFields);
    }

    @Test
    public void testWrite() {
        try (ArrowFormatWriter writer = new ArrowFormatWriter(PRIMITIVE_TYPE, 4096)) {
            List<InternalRow> list = new ArrayList<>();
            List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

            for (int i = 0; i < PRIMITIVE_TYPE.getFieldCount(); i++) {
                fieldGetters.add(InternalRow.createFieldGetter(PRIMITIVE_TYPE.getTypeAt(i), i));
            }
            for (int i = 0; i < 1000; i++) {
                list.add(GenericRow.of(randomRowValues(null)));
            }

            list.forEach(writer::write);

            writer.flush();
            VectorSchemaRoot vectorSchemaRoot = writer.getVectorSchemaRoot();

            ArrowBatchReader arrowBatchReader = new ArrowBatchReader(PRIMITIVE_TYPE);
            Iterable<InternalRow> rows = arrowBatchReader.readBatch(vectorSchemaRoot);

            Iterator<InternalRow> iterator = rows.iterator();
            for (int i = 0; i < 1000; i++) {
                InternalRow actual = iterator.next();
                InternalRow expectec = list.get(i);

                for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
                    Assertions.assertThat(fieldGetter.getFieldOrNull(actual))
                            .isEqualTo(fieldGetter.getFieldOrNull(expectec));
                }
            }
        }
    }

    private Object[] randomRowValues(boolean[] nullable) {
        Object[] values = new Object[18];
        values[0] = BinaryString.fromString(StringUtils.getRandomString(RND, 10, 10));
        values[1] = BinaryString.fromString(StringUtils.getRandomString(RND, 1, 20));
        values[2] = RND.nextBoolean();
        values[3] = randomBytes(10, 10);
        values[4] = randomBytes(1, 20);
        values[5] = Decimal.fromBigDecimal(new BigDecimal("0.22"), 2, 2);
        values[6] = Decimal.fromBigDecimal(new BigDecimal("12312455.22"), 38, 2);
        values[7] = Decimal.fromBigDecimal(new BigDecimal("12455.1"), 10, 1);
        values[8] = (byte) RND.nextInt(Byte.MAX_VALUE);
        values[9] = (short) RND.nextInt(Short.MAX_VALUE);
        values[10] = RND.nextInt();
        values[11] = RND.nextLong();
        values[12] = RND.nextFloat();
        values[13] = RND.nextDouble();
        values[14] = RND.nextInt();
        values[15] = Timestamp.fromEpochMillis(RND.nextInt(1000));
        values[16] = Timestamp.fromEpochMillis(RND.nextInt(1000), RND.nextInt(1000) * 1000);
        values[17] = Timestamp.fromEpochMillis(RND.nextInt(1000), RND.nextInt(1000_000));

        for (int i = 0; i < 18; i++) {
            if (nullable != null && nullable[i] && RND.nextBoolean()) {
                values[i] = null;
            }
        }

        return values;
    }

    private byte[] randomBytes(int minLength, int maxLength) {
        int len = RND.nextInt(maxLength - minLength + 1) + minLength;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) RND.nextInt(10);
        }
        return bytes;
    }
}

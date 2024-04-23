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

package org.apache.paimon.hive;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Util class for generating random {@link GenericRow}. */
public class RandomGenericRowDataGenerator {

    public static final List<DataType> LOGICAL_TYPES =
            Arrays.asList(
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(5, 3),
                    DataTypes.DECIMAL(28, 6),
                    DataTypes.CHAR(10),
                    DataTypes.VARCHAR(10),
                    DataTypes.STRING(),
                    DataTypes.VARBINARY(Integer.MAX_VALUE),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                    DataTypes.ARRAY(DataTypes.BIGINT()),
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));

    public static final List<String> TYPE_NAMES =
            Arrays.asList(
                    "boolean",
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "float",
                    "double",
                    "decimal(5,3)",
                    "decimal(28,6)",
                    "char(10)",
                    "varchar(10)",
                    "string",
                    "binary",
                    "date",
                    "string",
                    "timestamp",
                    "timestamp",
                    "array<bigint>",
                    "map<string,int>");

    public static final List<String> FIELD_NAMES =
            Arrays.asList(
                    "f_boolean",
                    "f_byte",
                    "f_short",
                    "f_int",
                    "f_long",
                    "f_float",
                    "f_double",
                    "f_decimal_5_3",
                    "f_decimal_28_6",
                    "f_char_10",
                    "f_varchar_10",
                    "f_string",
                    "f_binary",
                    "f_date",
                    "f_time",
                    "f_timestamp",
                    "f_timestamp_ltz",
                    "f_list_long",
                    "f_map_string_int");

    public static final List<String> FIELD_COMMENTS =
            Arrays.asList(
                    "comment_boolean",
                    "comment_byte",
                    "comment_short",
                    "comment_int",
                    "comment_long",
                    "comment_float",
                    "comment_double",
                    "comment_decimal_5_3",
                    "comment_decimal_28_6",
                    "comment_char_10",
                    "comment_varchar_10",
                    "comment_string",
                    "comment_binary",
                    "comment_date",
                    "comment_time",
                    "comment_timestamp",
                    "comment_timestamp_ltz",
                    "comment_list_long",
                    "comment_map_string_int");

    public static final RowType ROW_TYPE =
            new RowType(
                    IntStream.range(0, FIELD_NAMES.size())
                            .mapToObj(
                                    i ->
                                            new DataField(
                                                    i,
                                                    FIELD_NAMES.get(i),
                                                    LOGICAL_TYPES.get(i),
                                                    FIELD_COMMENTS.get(i)))
                            .collect(Collectors.toList()));

    // todo generate with schema
    public static GenericRow generate() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] randomBytes = new byte[random.nextInt(20)];
        random.nextBytes(randomBytes);
        Long[] randomLongArray = new Long[random.nextInt(20)];
        for (int i = 0; i < randomLongArray.length; i++) {
            if (random.nextBoolean()) {
                randomLongArray[i] = null;
            } else {
                randomLongArray[i] = random.nextLong();
            }
        }
        Map<BinaryString, Integer> randomMap = new HashMap<>();
        for (int i = random.nextInt(20); i > 0; i--) {
            randomMap.put(BinaryString.fromString(randomString(20)), random.nextInt());
        }
        GenericRow rowData =
                GenericRow.of(
                        random.nextBoolean(),
                        (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1),
                        (short) random.nextInt(Short.MIN_VALUE, Short.MAX_VALUE + 1),
                        random.nextInt(),
                        random.nextLong(),
                        random.nextFloat(),
                        random.nextDouble(),
                        Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3),
                        Decimal.fromBigDecimal(randomBigDecimal(28, 6), 28, 6),
                        BinaryString.fromString(randomString(10)),
                        BinaryString.fromString(randomString(10)),
                        BinaryString.fromString(randomString(100)),
                        randomBytes,
                        random.nextInt(10000),
                        random.nextInt(86400),
                        Timestamp.fromEpochMillis(random.nextLong(Integer.MAX_VALUE)),
                        Timestamp.fromEpochMillis(random.nextLong(Integer.MAX_VALUE)),
                        new GenericArray(randomLongArray),
                        new GenericMap(randomMap));
        for (int i = 0; i < rowData.getFieldCount(); i++) {
            if (random.nextBoolean()) {
                rowData.setField(i, null);
            }
        }
        return rowData;
    }

    private static String randomString(int lengthBound) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder builder = new StringBuilder();
        for (int i = random.nextInt(lengthBound); i >= 0; i--) {
            builder.append((char) random.nextInt('a', 'z' + 1));
        }
        return builder.toString();
    }

    public static BigDecimal randomBigDecimal(int precision, int scale) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder builder = new StringBuilder();
        // to avoid starting with  0
        for (int i = 0; i < precision - scale; i++) {
            int t = random.nextInt(10);
            builder.append(t == 0 ? 1 : t);
        }
        builder.append('.');
        for (int i = 0; i < scale; i++) {
            builder.append((char) (random.nextInt(10) + '0'));
        }
        return new BigDecimal(builder.toString());
    }
}

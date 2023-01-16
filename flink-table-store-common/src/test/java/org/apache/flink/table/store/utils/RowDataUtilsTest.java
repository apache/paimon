/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.Decimal;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.RowDataSerializer;
import org.apache.flink.table.store.data.Timestamp;
import org.apache.flink.table.store.datagen.DataGenerator;
import org.apache.flink.table.store.datagen.RandomGeneratorVisitor;
import org.apache.flink.table.store.datagen.RowDataGenerator;
import org.apache.flink.table.store.types.DataTypeRoot;
import org.apache.flink.table.store.types.DataTypes;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowDataUtils}. */
public class RowDataUtilsTest {

    public static final RowType ROW_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT().notNull())
                    .field("name", DataTypes.STRING()) /* optional by default */
                    .field("salary", DataTypes.DOUBLE().notNull())
                    .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
                    .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
                    .field("char", DataTypes.CHAR(10).notNull())
                    .field("varchar", DataTypes.VARCHAR(10).notNull())
                    .field("boolean", DataTypes.BOOLEAN().nullable())
                    .field("tinyint", DataTypes.TINYINT())
                    .field("smallint", DataTypes.SMALLINT())
                    .field("bigint", DataTypes.BIGINT())
                    .field("timestampWithoutZone", DataTypes.TIMESTAMP())
                    .field("timestampWithZone", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                    .field("date", DataTypes.DATE())
                    .field("decimal", DataTypes.DECIMAL(2, 2))
                    .field("decimal2", DataTypes.DECIMAL(38, 2))
                    .field("decimal3", DataTypes.DECIMAL(10, 1))
                    .build();

    private RowDataGenerator rowDataGenerator;

    private RowDataSerializer serializer;

    @BeforeEach
    public void before() throws Exception {
        DataGenerator[] generators =
                ROW_TYPE.getFields().stream()
                        .map(
                                field ->
                                        field.type()
                                                .accept(
                                                        new RandomGeneratorVisitor(
                                                                field.name(), new Configuration()))
                                                .getGenerator())
                        .toArray(DataGenerator[]::new);
        this.rowDataGenerator = new RowDataGenerator(generators);
        this.rowDataGenerator.open();
        this.serializer = new RowDataSerializer(ROW_TYPE);
    }

    @Test
    public void testCopy() {
        for (int i = 0; i < 10; i++) {
            InternalRow row = rowDataGenerator.next();
            InternalRow copied = RowDataUtils.copyRowData(row, ROW_TYPE);
            assertThat(toBinary(copied)).isEqualTo(toBinary(row));
            InternalRow copied2 = serializer.copy(row);

            // check copied
            for (int j = 0; j < copied.getFieldCount(); j++) {
                Object origin = RowDataUtils.get(row, j, ROW_TYPE.getTypeAt(j));
                Object field1 = RowDataUtils.get(copied, j, ROW_TYPE.getTypeAt(j));
                Object field2 = RowDataUtils.get(copied2, j, ROW_TYPE.getTypeAt(j));

                if (field2 != origin) {
                    assertThat(field1).isNotSameAs(origin);
                }
            }
        }
    }

    private BinaryRow toBinary(InternalRow row) {
        return serializer.toBinaryRow(row).copy();
    }

    @Test
    public void testCompare() {
        // test DECIMAL data type
        Decimal xDecimalData = Decimal.fromBigDecimal(new BigDecimal("12.34"), 4, 2);
        Decimal yDecimalData = Decimal.fromBigDecimal(new BigDecimal("13.14"), 4, 2);
        assertThat(RowDataUtils.compare(xDecimalData, yDecimalData, DataTypeRoot.DECIMAL))
                .isLessThan(0);

        // test DOUBLE data type
        double xDouble = 13.14;
        double yDouble = 12.13;
        assertThat(RowDataUtils.compare(xDouble, yDouble, DataTypeRoot.DOUBLE)).isGreaterThan(0);

        // test TIMESTAMP_WITHOUT_TIME_ZONE data type
        Timestamp xTimestamp = Timestamp.fromLocalDateTime(LocalDateTime.now());
        Timestamp yTimestamp = Timestamp.fromSQLTimestamp(xTimestamp.toSQLTimestamp());
        assertThat(
                        RowDataUtils.compare(
                                xTimestamp, yTimestamp, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE))
                .isEqualTo(0);
    }
}

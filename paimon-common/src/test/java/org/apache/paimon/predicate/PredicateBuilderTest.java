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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for {@link PredicateBuilder}. */
public class PredicateBuilderTest {

    @Test
    public void testBetween() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.between(0, 1, 3);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(2, 5, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 2, 0L)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testBetweenNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        Predicate predicate = builder.between(0, 1, null);

        assertThat(predicate.test(new Object[] {1})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {2})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(2, 5, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 2, 0L)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0L)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1L)}))
                .isEqualTo(false);
    }

    @Test
    public void testSplitAnd() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        RowType.of(
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType(),
                                new IntType()));

        Predicate child1 =
                PredicateBuilder.or(builder.isNull(0), builder.isNull(1), builder.isNull(2));
        Predicate child2 =
                PredicateBuilder.and(builder.isNull(3), builder.isNull(4), builder.isNull(5));
        Predicate child3 = builder.isNull(6);

        assertThat(PredicateBuilder.splitAnd(PredicateBuilder.and(child1, child2, child3)))
                .isEqualTo(
                        Arrays.asList(
                                child1,
                                builder.isNull(3),
                                builder.isNull(4),
                                builder.isNull(5),
                                child3));
    }

    @Test
    public void testConvertJavaObject() {
        assertEquals(PredicateBuilder.convertJavaObject(new IntType(), null), null);
        assertEquals(PredicateBuilder.convertJavaObject(new IntType(), 1), 1);
        BigDecimal bd = new BigDecimal(1.11);
        assertEquals(
                PredicateBuilder.convertJavaObject(new DecimalType(2, 1), bd),
                Decimal.fromBigDecimal(bd, 2, 1));
        assertEquals(PredicateBuilder.convertJavaObject(new BooleanType(), true), true);
        assertEquals(PredicateBuilder.convertJavaObject(new TinyIntType(), 1), (byte) 1);
        assertEquals(PredicateBuilder.convertJavaObject(new SmallIntType(), 2), (short) 2);
        assertEquals(
                PredicateBuilder.convertJavaObject(new BigIntType(), 1684814400000L),
                1684814400000L);
        assertEquals(PredicateBuilder.convertJavaObject(new FloatType(), 3.4), (float) 3.4);
        assertEquals(PredicateBuilder.convertJavaObject(new DoubleType(), 4.567382), 4.567382);
        assertEquals(
                PredicateBuilder.convertJavaObject(new VarCharType(), "varchar"),
                BinaryString.fromString("varchar"));
        assertEquals(
                PredicateBuilder.convertJavaObject(new DateType(), new Timestamp(1684814400000L)),
                19500);
        assertEquals(
                PredicateBuilder.convertJavaObject(new DateType(), new Date(123, 5, 13)), 19521);
        assertEquals(
                PredicateBuilder.convertJavaObject(new DateType(), LocalDate.of(2023, 6, 13)),
                19521);
        assertThrows(
                UnsupportedOperationException.class,
                () -> PredicateBuilder.convertJavaObject(new DateType(), 23));
        assertEquals(
                PredicateBuilder.convertJavaObject(new TimeType(), new Time(0, 30, 0)), 1800000);
        assertEquals(
                PredicateBuilder.convertJavaObject(new TimeType(), LocalTime.of(0, 30)), 1800000);
        assertThrows(
                UnsupportedOperationException.class,
                () -> PredicateBuilder.convertJavaObject(new TimeType(), 23));
        assertEquals(
                PredicateBuilder.convertJavaObject(
                        new TimestampType(), new Timestamp(1684814400000L)),
                org.apache.paimon.data.Timestamp.fromSQLTimestamp(new Timestamp(1684814400000L)));
        assertEquals(
                PredicateBuilder.convertJavaObject(
                        new LocalZonedTimestampType(), Instant.ofEpochMilli(1684814400000L)),
                org.apache.paimon.data.Timestamp.fromEpochMillis(1684814400000L));
        assertEquals(
                PredicateBuilder.convertJavaObject(
                        new TimestampType(), LocalDateTime.of(2023, 6, 13, 4, 34, 23)),
                org.apache.paimon.data.Timestamp.fromEpochMillis(1686630863000L));
        assertThrows(
                UnsupportedOperationException.class,
                () -> PredicateBuilder.convertJavaObject(new LocalZonedTimestampType(), 23));
        assertThrows(
                UnsupportedOperationException.class,
                () -> PredicateBuilder.convertJavaObject(new CharType(), "char"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> PredicateBuilder.convertJavaObject(new BinaryType(), "binary"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> PredicateBuilder.convertJavaObject(new VarBinaryType(), "varbinary"));
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        PredicateBuilder.convertJavaObject(
                                new MapType(new IntType(), new IntType()),
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("mapKey"),
                                                BinaryString.fromString("mapVal")))));
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        PredicateBuilder.convertJavaObject(
                                new ArrayType(new VarCharType(8)),
                                new GenericArray(
                                        new BinaryString[] {
                                            BinaryString.fromString("a"),
                                            BinaryString.fromString("b")
                                        })));
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        PredicateBuilder.convertJavaObject(
                                new MultisetType(new VarCharType(8)),
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("multiset"), 1))));
    }
}

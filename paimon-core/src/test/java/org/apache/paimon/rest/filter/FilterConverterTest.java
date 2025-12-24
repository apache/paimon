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

package org.apache.paimon.rest.filter;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataTypes;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FilterPredicateConverter}. */
public class FilterConverterTest {

    @Test
    public void testTransformPredicateConversion() {
        TransformFilter rest =
                new TransformFilter(
                        new FieldFilterTransform(0, "a", DataTypes.INT()),
                        LeafFilterFunction.EQUAL,
                        Collections.singletonList(5));

        Predicate predicate = FilterPredicateConverter.toPredicate(rest);
        assertNotNull(predicate);

        assertTrue(predicate.test(GenericRow.of(5)));
        assertFalse(predicate.test(GenericRow.of(6)));
    }

    @Test
    public void testCompoundPredicateConversion() {
        Filter p1 =
                new TransformFilter(
                        new FieldFilterTransform(0, "a", DataTypes.INT()),
                        LeafFilterFunction.EQUAL,
                        Collections.singletonList(5));
        Filter p2 =
                new TransformFilter(
                        new FieldFilterTransform(1, "b", DataTypes.STRING()),
                        LeafFilterFunction.EQUAL,
                        Collections.singletonList("x"));
        CompoundFilter rest = new CompoundFilter(CompoundFilterFunction.AND, Arrays.asList(p1, p2));

        Predicate predicate = FilterPredicateConverter.toPredicate(rest);
        assertNotNull(predicate);

        assertTrue(predicate.test(GenericRow.of(5, BinaryString.fromString("x"))));
        assertFalse(predicate.test(GenericRow.of(5, BinaryString.fromString("y"))));
        assertFalse(predicate.test(GenericRow.of(6, BinaryString.fromString("x"))));
    }

    @Test
    public void testOrCompoundPredicateConversion() {
        Filter p1 =
                new TransformFilter(
                        new FieldFilterTransform(0, "a", DataTypes.INT()),
                        LeafFilterFunction.EQUAL,
                        Collections.singletonList(5));
        Filter p2 =
                new TransformFilter(
                        new FieldFilterTransform(0, "a", DataTypes.INT()),
                        LeafFilterFunction.EQUAL,
                        Collections.singletonList(6));
        CompoundFilter rest = new CompoundFilter(CompoundFilterFunction.OR, Arrays.asList(p1, p2));

        Predicate predicate = FilterPredicateConverter.toPredicate(rest);
        assertNotNull(predicate);

        assertTrue(predicate.test(GenericRow.of(5)));
        assertTrue(predicate.test(GenericRow.of(6)));
        assertFalse(predicate.test(GenericRow.of(7)));
    }

    @Test
    public void testInFunctionConversion() {
        TransformFilter rest =
                new TransformFilter(
                        new FieldFilterTransform(0, "a", DataTypes.INT()),
                        LeafFilterFunction.IN,
                        Arrays.asList(1, 2, 3));

        Predicate predicate = FilterPredicateConverter.toPredicate(rest);
        assertNotNull(predicate);

        assertTrue(predicate.test(GenericRow.of(2)));
        assertFalse(predicate.test(GenericRow.of(5)));
    }

    @Test
    public void testIsNullFunctionConversion() {
        TransformFilter rest =
                new TransformFilter(
                        new FieldFilterTransform(0, "a", DataTypes.STRING()),
                        LeafFilterFunction.IS_NULL,
                        null);

        Predicate predicate = FilterPredicateConverter.toPredicate(rest);
        assertNotNull(predicate);

        assertTrue(predicate.test(GenericRow.of((Object) null)));
        assertFalse(predicate.test(GenericRow.of(BinaryString.fromString("x"))));
    }

    @Test
    public void testLiteralConversions() {
        // BOOLEAN from string
        Predicate boolEq =
                FilterPredicateConverter.toPredicate(
                        new TransformFilter(
                                new FieldFilterTransform(0, "b", DataTypes.BOOLEAN()),
                                LeafFilterFunction.EQUAL,
                                Collections.singletonList("true")));
        assertNotNull(boolEq);
        assertTrue(boolEq.test(GenericRow.of(true)));
        assertFalse(boolEq.test(GenericRow.of(false)));

        // VARBINARY/BINARY from base64 string
        byte[] bytes = new byte[] {1, 2, 3};
        String b64 = Base64.getEncoder().encodeToString(bytes);
        Predicate bytesEq =
                FilterPredicateConverter.toPredicate(
                        new TransformFilter(
                                new FieldFilterTransform(0, "c", DataTypes.VARBINARY(3)),
                                LeafFilterFunction.EQUAL,
                                Collections.singletonList(b64)));
        assertNotNull(bytesEq);
        assertTrue(bytesEq.test(GenericRow.of(bytes)));
        assertFalse(bytesEq.test(GenericRow.of(new byte[] {1, 2, 4})));

        // DECIMAL from string
        Decimal expectedDec = Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2);
        Predicate decEq =
                FilterPredicateConverter.toPredicate(
                        new TransformFilter(
                                new FieldFilterTransform(0, "d", DataTypes.DECIMAL(10, 2)),
                                LeafFilterFunction.EQUAL,
                                Collections.singletonList("12.34")));
        assertNotNull(decEq);
        assertTrue(decEq.test(GenericRow.of(expectedDec)));
        assertFalse(
                decEq.test(GenericRow.of(Decimal.fromBigDecimal(new BigDecimal("12.35"), 10, 2))));

        // DATE from ISO string -> epoch day int
        LocalDate date = LocalDate.parse("2025-12-23");
        int epochDay = (int) date.toEpochDay();
        Predicate dateEq =
                FilterPredicateConverter.toPredicate(
                        new TransformFilter(
                                new FieldFilterTransform(0, "e", DataTypes.DATE()),
                                LeafFilterFunction.EQUAL,
                                Collections.singletonList("2025-12-23")));
        assertNotNull(dateEq);
        assertTrue(dateEq.test(GenericRow.of(epochDay)));
        assertFalse(dateEq.test(GenericRow.of(epochDay + 1)));

        // TIMESTAMP from string (java.sql.Timestamp.valueOf compatible)
        Timestamp ts = Timestamp.fromEpochMillis(1_700_000_000_000L);
        String tsString = ts.toSQLTimestamp().toString();
        Predicate tsEq =
                FilterPredicateConverter.toPredicate(
                        new TransformFilter(
                                new FieldFilterTransform(0, "f", DataTypes.TIMESTAMP_MILLIS()),
                                LeafFilterFunction.EQUAL,
                                Collections.singletonList(tsString)));
        assertNotNull(tsEq);
        assertTrue(tsEq.test(GenericRow.of(ts)));
        assertFalse(tsEq.test(GenericRow.of(Timestamp.fromEpochMillis(ts.getMillisecond() + 1))));
    }

    @Test
    public void testDecimalOverflowThrows() {
        TransformFilter rest =
                new TransformFilter(
                        new FieldFilterTransform(0, "d", DataTypes.DECIMAL(2, 0)),
                        LeafFilterFunction.EQUAL,
                        Collections.singletonList("123"));

        assertThrows(
                IllegalArgumentException.class, () -> FilterPredicateConverter.toPredicate(rest));
    }
}

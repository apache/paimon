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

package org.apache.paimon.statistics;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.BinaryStringSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SimpleColStatsCollector}. */
public class SimpleColStatsCollectorTest {

    private Serializer<Object>[] serializers;
    private static final String s1 = StringUtils.repeat("a", 12);
    private static final String s1_t = "aa";
    private static final String s2 = StringUtils.repeat("b", 12);
    private static final String s3 = StringUtils.repeat("d", 13);
    private static final String s3_t = "de";

    @BeforeEach
    public void before() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType(), "Someone's desc."),
                                new DataField(1, "b", new VarCharType())));
        serializers = new Serializer[2];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            serializers[i] = InternalSerializers.create(rowType.getTypeAt(i));
        }
    }

    @Test
    public void testParse() {
        assertThat(
                        SimpleColStatsCollector.from("none").create()
                                instanceof NoneSimpleColStatsCollector)
                .isTrue();
        assertThat(
                        SimpleColStatsCollector.from("Full").create()
                                instanceof FullSimpleColStatsCollector)
                .isTrue();
        assertThat(
                        SimpleColStatsCollector.from("CoUNts").create()
                                instanceof CountsSimpleColStatsCollector)
                .isTrue();
        TruncateSimpleColStatsCollector t1 =
                (TruncateSimpleColStatsCollector)
                        SimpleColStatsCollector.from("truncate(10)").create();
        assertThat(t1.getLength()).isEqualTo(10);
        assertThatThrownBy(() -> SimpleColStatsCollector.from("aatruncate(10)"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SimpleColStatsCollector.from("truncate(10.1)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNone() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new SimpleColStats(null, null, null),
                new SimpleColStats(1, 4, 0L),
                new NoneSimpleColStatsCollector());
        check(
                rows,
                1,
                new SimpleColStats(null, null, null),
                new SimpleColStats(s1, s3, 1L),
                new NoneSimpleColStatsCollector());
    }

    @Test
    public void testCounts() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new SimpleColStats(null, null, 0L, 0L),
                new SimpleColStats(1, 4, 0L, 0L),
                new CountsSimpleColStatsCollector());
        check(
                rows,
                1,
                new SimpleColStats(null, null, 1L, 0L),
                new SimpleColStats(s1, s3, 1L, 0L),
                new CountsSimpleColStatsCollector());
    }

    @Test
    public void testFull() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new SimpleColStats(1, 4, 0L, 0L),
                new SimpleColStats(1, 4, 0L, 0L),
                new FullSimpleColStatsCollector());
        check(
                rows,
                1,
                new SimpleColStats(
                        BinaryString.fromString(s1), BinaryString.fromString(s3), 1L, 0L),
                new SimpleColStats(
                        BinaryString.fromString(s1), BinaryString.fromString(s3), 1L, 0L),
                new FullSimpleColStatsCollector());
    }

    @Test
    public void testTruncate() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new SimpleColStats(1, 4, 0L, 0L),
                new SimpleColStats(1, 4, 0L, 0L),
                new TruncateSimpleColStatsCollector(1));
        check(
                rows,
                1,
                new SimpleColStats(
                        BinaryString.fromString(s1_t), BinaryString.fromString(s3_t), 1L, 0L),
                new SimpleColStats(
                        BinaryString.fromString(s1), BinaryString.fromString(s3), 1L, 0L),
                new TruncateSimpleColStatsCollector(2));
    }

    @Test
    public void testFullCountsNaNAndExcludesFromBounds() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "d", new DoubleType()),
                                new DataField(1, "f", new FloatType())));
        Serializer<Object>[] floatSerializers = new Serializer[2];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            floatSerializers[i] = InternalSerializers.create(rowType.getTypeAt(i));
        }

        FullSimpleColStatsCollector doubleCollector = new FullSimpleColStatsCollector();
        doubleCollector.collect(1.0d, floatSerializers[0]);
        doubleCollector.collect(Double.NaN, floatSerializers[0]);
        doubleCollector.collect(5.0d, floatSerializers[0]);
        doubleCollector.collect(Double.NaN, floatSerializers[0]);
        doubleCollector.collect(null, floatSerializers[0]);
        assertThat(doubleCollector.result()).isEqualTo(new SimpleColStats(1.0d, 5.0d, 1L, 2L));

        FullSimpleColStatsCollector floatCollector = new FullSimpleColStatsCollector();
        floatCollector.collect(2.0f, floatSerializers[1]);
        floatCollector.collect(Float.NaN, floatSerializers[1]);
        floatCollector.collect(7.0f, floatSerializers[1]);
        assertThat(floatCollector.result()).isEqualTo(new SimpleColStats(2.0f, 7.0f, 0L, 1L));
    }

    @Test
    public void testCountsNaN() {
        Serializer<Object> doubleSerializer = InternalSerializers.create(new DoubleType());
        CountsSimpleColStatsCollector collector = new CountsSimpleColStatsCollector();
        collector.collect(1.0d, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        collector.collect(null, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        assertThat(collector.result()).isEqualTo(new SimpleColStats(null, null, 1L, 2L));
    }

    @Test
    public void testFullAllNaN() {
        Serializer<Object> doubleSerializer = InternalSerializers.create(new DoubleType());
        FullSimpleColStatsCollector collector = new FullSimpleColStatsCollector();
        collector.collect(Double.NaN, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        assertThat(collector.result()).isEqualTo(new SimpleColStats(null, null, 0L, 3L));
    }

    @Test
    public void testFullOnlyNaNAndNull() {
        Serializer<Object> doubleSerializer = InternalSerializers.create(new DoubleType());
        FullSimpleColStatsCollector collector = new FullSimpleColStatsCollector();
        collector.collect(null, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        collector.collect(null, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        collector.collect(null, doubleSerializer);
        assertThat(collector.result()).isEqualTo(new SimpleColStats(null, null, 3L, 2L));
    }

    @Test
    public void testNoneIgnoresNaN() {
        Serializer<Object> doubleSerializer = InternalSerializers.create(new DoubleType());
        NoneSimpleColStatsCollector collector = new NoneSimpleColStatsCollector();
        collector.collect(Double.NaN, doubleSerializer);
        collector.collect(1.0d, doubleSerializer);
        collector.collect(Double.NaN, doubleSerializer);
        assertThat(collector.result()).isEqualTo(SimpleColStats.NONE);
        assertThat(collector.result().nanCount()).isNull();
    }

    @Test
    public void testConvertPreservesNanCount() {
        SimpleColStats source = new SimpleColStats(1.0d, 5.0d, 2L, 7L);
        assertThat(new FullSimpleColStatsCollector().convert(source).nanCount()).isEqualTo(7L);
        assertThat(new CountsSimpleColStatsCollector().convert(source).nanCount()).isEqualTo(7L);
        assertThat(new TruncateSimpleColStatsCollector(16).convert(source).nanCount())
                .isEqualTo(7L);
        assertThat(new NoneSimpleColStatsCollector().convert(source).nanCount()).isNull();
    }

    @Test
    public void testSimpleColStatsEqualityIncludesNanCount() {
        assertThat(new SimpleColStats(1.0d, 5.0d, 0L, 0L))
                .isNotEqualTo(new SimpleColStats(1.0d, 5.0d, 0L, 1L));
        assertThat(new SimpleColStats(1.0d, 5.0d, 0L, 0L))
                .isNotEqualTo(new SimpleColStats(1.0d, 5.0d, 0L, null));
        assertThat(new SimpleColStats(1.0d, 5.0d, 0L, 7L))
                .isEqualTo(new SimpleColStats(1.0d, 5.0d, 0L, 7L));
    }

    @Test
    public void testTruncateTwoChar() {
        TruncateSimpleColStatsCollector t1 = new TruncateSimpleColStatsCollector(1);
        SimpleColStats fieldStats =
                new SimpleColStats(
                        BinaryString.fromString("\uD83E\uDD18a"),
                        BinaryString.fromString("\uD83E\uDD18b"),
                        0L);
        fieldStats = t1.convert(fieldStats);
        assertThat(fieldStats.min()).isEqualTo(BinaryString.fromString("\uD83E\uDD18"));
        assertThat(fieldStats.max()).isEqualTo(BinaryString.fromString("\uD83E\uDD19"));
    }

    @Test
    public void testTruncateCopied() {
        TruncateSimpleColStatsCollector collector = new TruncateSimpleColStatsCollector(16);
        BinaryString str = BinaryString.fromString("str");
        collector.collect(str, (Serializer) BinaryStringSerializer.INSTANCE);
        SimpleColStats stats = collector.result();
        assertThat(stats.min()).isNotSameAs(str);
        assertThat(stats.max()).isNotSameAs(str);
    }

    @Test
    public void testFullCopied() {
        FullSimpleColStatsCollector collector = new FullSimpleColStatsCollector();
        BinaryString str = BinaryString.fromString("str");
        collector.collect(str, (Serializer) BinaryStringSerializer.INSTANCE);
        SimpleColStats stats = collector.result();
        assertThat(stats.min()).isNotSameAs(str);
        assertThat(stats.max()).isNotSameAs(str);
    }

    @Test
    public void testTruncateFail() {
        TruncateSimpleColStatsCollector collector = new TruncateSimpleColStatsCollector(3);

        StringBuilder builder = new StringBuilder();
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        BinaryString str = BinaryString.fromString(builder.toString());

        collector.collect(str, (Serializer) BinaryStringSerializer.INSTANCE);
        SimpleColStats stats = collector.result();
        assertThat(stats.min()).isNull();
        assertThat(stats.max()).isNull();

        stats = collector.convert(new SimpleColStats(str, str, 0L));
        assertThat(stats.min()).isNull();
        assertThat(stats.max()).isNull();
    }

    private void check(
            List<GenericRow> rows,
            int column,
            SimpleColStats expected,
            SimpleColStats formatExtracted,
            SimpleColStatsCollector simpleColStatsCollector) {
        for (GenericRow row : rows) {
            simpleColStatsCollector.collect(row.getField(column), serializers[column]);
        }

        assertThat(simpleColStatsCollector.result()).isEqualTo(expected);
        assertThat(simpleColStatsCollector.convert(formatExtracted)).isEqualTo(expected);
    }

    private List<GenericRow> getRows() {
        List<GenericRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(1, BinaryString.fromString(s1)));
        rows.add(GenericRow.of(2, BinaryString.fromString(s2)));
        rows.add(GenericRow.of(3, null));
        rows.add(GenericRow.of(4, BinaryString.fromString(s3)));
        return rows;
    }
}

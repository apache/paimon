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
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.DataField;
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

/** Test for {@link FieldStatsCollector}. */
public class FieldStatsCollectorTest {

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
        assertThat(FieldStatsCollector.from("none").create() instanceof NoneFieldStatsCollector)
                .isTrue();
        assertThat(FieldStatsCollector.from("Full").create() instanceof FullFieldStatsCollector)
                .isTrue();
        assertThat(FieldStatsCollector.from("CoUNts").create() instanceof CountsFieldStatsCollector)
                .isTrue();
        TruncateFieldStatsCollector t1 =
                (TruncateFieldStatsCollector) FieldStatsCollector.from("truncate(10)").create();
        assertThat(t1.getLength()).isEqualTo(10);
        assertThatThrownBy(() -> FieldStatsCollector.from("aatruncate(10)"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FieldStatsCollector.from("truncate(10.1)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNone() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new FieldStats(null, null, null),
                new FieldStats(1, 4, 0L),
                new NoneFieldStatsCollector());
        check(
                rows,
                1,
                new FieldStats(null, null, null),
                new FieldStats(s1, s3, 1L),
                new NoneFieldStatsCollector());
    }

    @Test
    public void testCounts() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new FieldStats(null, null, 0L),
                new FieldStats(1, 4, 0L),
                new CountsFieldStatsCollector());
        check(
                rows,
                1,
                new FieldStats(null, null, 1L),
                new FieldStats(s1, s3, 1L),
                new CountsFieldStatsCollector());
    }

    @Test
    public void testFull() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new FieldStats(1, 4, 0L),
                new FieldStats(1, 4, 0L),
                new FullFieldStatsCollector());
        check(
                rows,
                1,
                new FieldStats(BinaryString.fromString(s1), BinaryString.fromString(s3), 1L),
                new FieldStats(BinaryString.fromString(s1), BinaryString.fromString(s3), 1L),
                new FullFieldStatsCollector());
    }

    @Test
    public void testTruncate() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(
                rows,
                0,
                new FieldStats(1, 4, 0L),
                new FieldStats(1, 4, 0L),
                new TruncateFieldStatsCollector(1));
        check(
                rows,
                1,
                new FieldStats(BinaryString.fromString(s1_t), BinaryString.fromString(s3_t), 1L),
                new FieldStats(BinaryString.fromString(s1), BinaryString.fromString(s3), 1L),
                new TruncateFieldStatsCollector(2));
    }

    @Test
    public void testTruncateTwoChar() {
        TruncateFieldStatsCollector t1 = new TruncateFieldStatsCollector(1);
        FieldStats fieldStats =
                new FieldStats(
                        BinaryString.fromString("\uD83E\uDD18a"),
                        BinaryString.fromString("\uD83E\uDD18b"),
                        0L);
        fieldStats = t1.convert(fieldStats);
        assertThat(fieldStats.minValue()).isEqualTo(BinaryString.fromString("\uD83E\uDD18"));
        assertThat(fieldStats.maxValue()).isEqualTo(BinaryString.fromString("\uD83E\uDD19"));
    }

    @Test
    public void testTruncateCopied() {
        TruncateFieldStatsCollector collector = new TruncateFieldStatsCollector(16);
        BinaryString str = BinaryString.fromString("str");
        collector.collect(str, (Serializer) BinaryStringSerializer.INSTANCE);
        FieldStats stats = collector.result();
        assertThat(stats.minValue()).isNotSameAs(str);
        assertThat(stats.maxValue()).isNotSameAs(str);
    }

    @Test
    public void testFullCopied() {
        FullFieldStatsCollector collector = new FullFieldStatsCollector();
        BinaryString str = BinaryString.fromString("str");
        collector.collect(str, (Serializer) BinaryStringSerializer.INSTANCE);
        FieldStats stats = collector.result();
        assertThat(stats.minValue()).isNotSameAs(str);
        assertThat(stats.maxValue()).isNotSameAs(str);
    }

    @Test
    public void testTruncateFail() {
        TruncateFieldStatsCollector collector = new TruncateFieldStatsCollector(3);

        StringBuilder builder = new StringBuilder();
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        builder.appendCodePoint(Character.MAX_CODE_POINT);
        BinaryString str = BinaryString.fromString(builder.toString());

        collector.collect(str, (Serializer) BinaryStringSerializer.INSTANCE);
        FieldStats stats = collector.result();
        assertThat(stats.minValue()).isNull();
        assertThat(stats.maxValue()).isNull();

        stats = collector.convert(new FieldStats(str, str, 0L));
        assertThat(stats.minValue()).isNull();
        assertThat(stats.maxValue()).isNull();
    }

    private void check(
            List<GenericRow> rows,
            int column,
            FieldStats expected,
            FieldStats formatExtracted,
            FieldStatsCollector fieldStatsCollector) {
        for (GenericRow row : rows) {
            fieldStatsCollector.collect(row.getField(column), serializers[column]);
        }

        assertThat(fieldStatsCollector.result()).isEqualTo(expected);
        assertThat(fieldStatsCollector.convert(formatExtracted)).isEqualTo(expected);
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

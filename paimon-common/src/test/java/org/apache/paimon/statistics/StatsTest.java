/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.statistics;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Test for {@link Stats}. */
public class StatsTest {

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
        Assertions.assertTrue(Stats.from("none") instanceof NoneStats);
        Assertions.assertTrue(Stats.from("Full") instanceof FullStats);
        Assertions.assertTrue(Stats.from("CoUNts") instanceof CountsStats);
        TruncateStats t1 = (TruncateStats) Stats.from("truncate(10)");
        Assertions.assertEquals(10, t1.getLength());
        assertThatThrownBy(() -> Stats.from("aatruncate(10)"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Stats.from("truncate(10.1)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNone() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(rows, 0, new FieldStats(null, null, null), new FieldStats(1, 4, 0L), new NoneStats());
        check(
                rows,
                1,
                new FieldStats(null, null, null),
                new FieldStats(s1, s3, 1L),
                new NoneStats());
    }

    @Test
    public void testCounts() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(rows, 0, new FieldStats(null, null, 0L), new FieldStats(1, 4, 0L), new CountsStats());
        check(
                rows,
                1,
                new FieldStats(null, null, 1L),
                new FieldStats(s1, s3, 1L),
                new CountsStats());
    }

    @Test
    public void testFull() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(rows, 0, new FieldStats(1, 4, 0L), new FieldStats(1, 4, 0L), new FullStats());
        check(
                rows,
                1,
                new FieldStats(BinaryString.fromString(s1), BinaryString.fromString(s3), 1L),
                new FieldStats(BinaryString.fromString(s1), BinaryString.fromString(s3), 1L),
                new FullStats());
    }

    @Test
    public void testTruncate() {
        List<GenericRow> rows = getRows();
        for (int i = 0; i < serializers.length; i++) {}
        check(rows, 0, new FieldStats(1, 4, 0L), new FieldStats(1, 4, 0L), new TruncateStats(1));
        check(
                rows,
                1,
                new FieldStats(BinaryString.fromString(s1_t), BinaryString.fromString(s3_t), 1L),
                new FieldStats(BinaryString.fromString(s1), BinaryString.fromString(s3), 1L),
                new TruncateStats(2));
    }

    @Test
    public void testTruncateTwoChar() {
        TruncateStats t1 = new TruncateStats(1);
        FieldStats fieldStats =
                new FieldStats(
                        BinaryString.fromString("\uD83E\uDD18a"),
                        BinaryString.fromString("\uD83E\uDD18b"),
                        0L);
        fieldStats = t1.convert(fieldStats);
        Assertions.assertEquals(BinaryString.fromString("\uD83E\uDD18"), fieldStats.minValue());
        Assertions.assertEquals(BinaryString.fromString("\uD83E\uDD19"), fieldStats.maxValue());
    }

    private void check(
            List<GenericRow> rows,
            int column,
            FieldStats expected,
            FieldStats formatExtracted,
            Stats stats) {
        for (GenericRow row : rows) {
            stats.collect(row.getField(column), serializers[column]);
        }
        Assertions.assertEquals(expected, stats.result());
        Assertions.assertEquals(expected, stats.convert(formatExtracted));
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

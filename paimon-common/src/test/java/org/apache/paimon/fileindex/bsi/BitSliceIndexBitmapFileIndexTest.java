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

package org.apache.paimon.fileindex.bsi;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** test for {@link BitSliceIndexBitmapFileIndex}. */
public class BitSliceIndexBitmapFileIndexTest {

    @Test
    public void testBitSliceIndexMix() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(intType);
        FileIndexWriter writer = bsiFileIndex.createWriter();

        Object[] arr = {1, 2, null, -2, -2, -1, null, 2, 0, 5, null};

        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bsiFileIndex.createReader(stream, 0, bytes.length);

        // test eq
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 7));
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4));
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 100)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // test neq
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 3, 4, 5, 8, 9));
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 5, 7, 8, 9));
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, 100)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 7, 8, 9));

        // test in
        assertThat(((BitmapIndexResult) reader.visitIn(fieldRef, Arrays.asList(-1, 1, 2, 3))).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 5, 7));

        // test not in
        assertThat(
                        ((BitmapIndexResult)
                                        reader.visitNotIn(fieldRef, Arrays.asList(-1, 1, 2, 3)))
                                .get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4, 8, 9));

        // test null
        assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(2, 6, 10));

        // test is not null
        assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 7, 8, 9));

        // test lt
        assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 3, 4, 5, 8));
        assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 7, 8));
        assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, -1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4));
        assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, -1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4, 5));

        // test gt
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 5, 7, 8, 9));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 7, 8, 9));
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(9));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 7, 9));
    }

    @Test
    public void testBitSliceIndexPositiveOnly() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(intType);
        FileIndexWriter writer = bsiFileIndex.createWriter();

        Object[] arr = {0, 1, null, 3, 4, 5, 6, 0, null};

        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bsiFileIndex.createReader(stream, 0, bytes.length);

        // test eq
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 7));
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1));
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, -1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // test neq
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 6, 7));
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 6, 7));
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, 3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 4, 5, 6, 7));

        // test in
        assertThat(((BitmapIndexResult) reader.visitIn(fieldRef, Arrays.asList(-1, 1, 2, 3))).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3));

        // test not in
        assertThat(
                        ((BitmapIndexResult)
                                        reader.visitNotIn(fieldRef, Arrays.asList(-1, 1, 2, 3)))
                                .get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 4, 5, 6, 7));

        // test null
        assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(2, 8));

        // test is not null
        assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 6, 7));

        // test lt
        assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, 3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 7));
        assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, 3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 7));
        assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, -1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, -1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // test gt
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 6, 7));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5, 6, 7));
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4, 5, 6));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3, 4, 5, 6));
    }

    @Test
    public void testBitSliceIndexNegativeOnly() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(intType);
        FileIndexWriter writer = bsiFileIndex.createWriter();

        Object[] arr = {null, -1, null, -3, -4, -5, -6, -1, null};

        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bsiFileIndex.createReader(stream, 0, bytes.length);

        // test eq
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, -1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 7));

        // test neq
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, -2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3, 4, 5, 6, 7));
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, -3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 4, 5, 6, 7));

        // test in
        assertThat(
                        ((BitmapIndexResult) reader.visitIn(fieldRef, Arrays.asList(-1, -4, -2, 3)))
                                .get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 4, 7));

        // test not in
        assertThat(
                        ((BitmapIndexResult)
                                        reader.visitNotIn(fieldRef, Arrays.asList(-1, -4, -2, 3)))
                                .get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 5, 6));

        // test null
        assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 2, 8));

        // test is not null
        assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3, 4, 5, 6, 7));

        // test lt
        assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, -3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(4, 5, 6));
        assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, -3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4, 5, 6));
        assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3, 4, 5, 6, 7));
        assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3, 4, 5, 6, 7));

        // test gt
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, -3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 7));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, -3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 3, 7));
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
    }

    @Test
    public void testReaderPredicatePruningWithLongMinValue() {
        BigIntType bigIntType = new BigIntType();
        FieldRef fieldRef = new FieldRef(0, "", bigIntType);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(bigIntType);
        FileIndexWriter writer = bsiFileIndex.createWriter();

        // Use values that include negative numbers but NOT Long.MIN_VALUE itself
        // (since the writer cannot handle it). This isolates the reader-side bug.
        // Data: [-100, -1, null, 0, 1, 50]
        Object[] arr = {-100L, -1L, null, 0L, 1L, 50L};

        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bsiFileIndex.createReader(stream, 0, bytes.length);

        // All non-null row ids: {0, 1, 3, 4, 5}

        // x > Long.MIN_VALUE: every int64 value > Long.MIN_VALUE (since no row IS Long.MIN_VALUE),
        // so result should be ALL non-null rows = {0, 1, 3, 4, 5}
        RoaringBitmap32 gtResult =
                ((BitmapIndexResult) reader.visitGreaterThan(fieldRef, Long.MIN_VALUE)).get();
        assertThat(gtResult)
                .as("x > Long.MIN_VALUE should return all non-null rows")
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5));

        // x >= Long.MIN_VALUE: same — all non-null rows satisfy this
        RoaringBitmap32 gteResult =
                ((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, Long.MIN_VALUE)).get();
        assertThat(gteResult)
                .as("x >= Long.MIN_VALUE should return all non-null rows")
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5));

        // x < Long.MIN_VALUE: no int64 value is less than Long.MIN_VALUE, so result should be
        // empty
        RoaringBitmap32 ltResult =
                ((BitmapIndexResult) reader.visitLessThan(fieldRef, Long.MIN_VALUE)).get();
        assertThat(ltResult)
                .as("x < Long.MIN_VALUE should return empty")
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // x <= Long.MIN_VALUE: no row has Long.MIN_VALUE, so result should be empty
        RoaringBitmap32 lteResult =
                ((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, Long.MIN_VALUE)).get();
        assertThat(lteResult)
                .as("x <= Long.MIN_VALUE should return empty (no row has that value)")
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // x == Long.MIN_VALUE: no row has Long.MIN_VALUE, so result should be empty
        RoaringBitmap32 eqResult =
                ((BitmapIndexResult) reader.visitEqual(fieldRef, Long.MIN_VALUE)).get();
        assertThat(eqResult)
                .as("x == Long.MIN_VALUE should return empty")
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // x != Long.MIN_VALUE: all non-null rows (no row has Long.MIN_VALUE)
        RoaringBitmap32 neqResult =
                ((BitmapIndexResult) reader.visitNotEqual(fieldRef, Long.MIN_VALUE)).get();
        assertThat(neqResult)
                .as("x != Long.MIN_VALUE should return all non-null rows")
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 3, 4, 5));
    }

    @Test
    public void testWriterCannotHandleLongMinValue() {
        BigIntType bigIntType = new BigIntType();
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(bigIntType);
        FileIndexWriter writer = bsiFileIndex.createWriter();
        writer.write(Long.MIN_VALUE);

        assertThatThrownBy(writer::serializedBytes)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("values should be non-negative");
    }

    @Test
    public void testSubMicrosecondTimestampIndexAnswersNoValuePredicate() {
        // The value mapper stores micros, so these two rows share one indexed value.
        Timestamp second = Timestamp.fromEpochMillis(1000, 0);
        Timestamp secondAndHalfMicro = Timestamp.fromEpochMillis(1000, 500);

        TimestampType nanos = new TimestampType(9);
        FieldRef fieldRef = new FieldRef(0, "", nanos);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(nanos);
        FileIndexWriter writer = bsiFileIndex.createWriter();
        for (Object o : new Object[] {second, secondAndHalfMicro, null}) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader =
                bsiFileIndex.createReader(new ByteArraySeekableStream(bytes), 0, bytes.length);

        // Answering these from the index would drop row 1 from the <> result and select it for
        // the =, since the bitmap is the row set the scan reads.
        assertThat(reader.visitEqual(fieldRef, second)).isSameAs(FileIndexResult.REMAIN);
        assertThat(reader.visitNotEqual(fieldRef, second)).isSameAs(FileIndexResult.REMAIN);
        assertThat(reader.visitIn(fieldRef, Arrays.asList(second, secondAndHalfMicro)))
                .isSameAs(FileIndexResult.REMAIN);
        assertThat(reader.visitNotIn(fieldRef, Arrays.asList(second)))
                .isSameAs(FileIndexResult.REMAIN);
        assertThat(reader.visitLessThan(fieldRef, secondAndHalfMicro))
                .isSameAs(FileIndexResult.REMAIN);
        assertThat(reader.visitGreaterThan(fieldRef, second)).isSameAs(FileIndexResult.REMAIN);
        assertThat(reader.visitBetween(fieldRef, second, secondAndHalfMicro))
                .isSameAs(FileIndexResult.REMAIN);

        // Null-ness does not depend on the truncated digits, so it still prunes.
        assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(2));
        assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1));
    }

    @Test
    public void testMicrosecondTimestampIndexStillAnswersValuePredicates() {
        // Precision 6 is exactly what the mapper stores, so nothing is given up there.
        Timestamp second = Timestamp.fromEpochMillis(1000, 0);
        Timestamp secondAndMicro = Timestamp.fromEpochMillis(1000, 1000);

        TimestampType micros = new TimestampType(6);
        FieldRef fieldRef = new FieldRef(0, "", micros);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(micros);
        FileIndexWriter writer = bsiFileIndex.createWriter();
        for (Object o : new Object[] {second, secondAndMicro}) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        FileIndexReader reader =
                bsiFileIndex.createReader(new ByteArraySeekableStream(bytes), 0, bytes.length);

        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, second)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0));
        assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, second)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1));
    }
}

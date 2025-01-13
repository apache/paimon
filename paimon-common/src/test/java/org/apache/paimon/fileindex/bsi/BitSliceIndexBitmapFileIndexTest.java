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

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** test for {@link BitSliceIndexBitmapFileIndex}. */
public class BitSliceIndexBitmapFileIndexTest {

    @Test
    public void testBitSliceIndexMix() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(intType, null);
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
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(intType, null);
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
        BitSliceIndexBitmapFileIndex bsiFileIndex = new BitSliceIndexBitmapFileIndex(intType, null);
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
}

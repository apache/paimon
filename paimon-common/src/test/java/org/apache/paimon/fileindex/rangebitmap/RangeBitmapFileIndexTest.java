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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** test for {@link RangeBitmapFileIndex}. */
public class RangeBitmapFileIndexTest {

    private static final int ROW_COUNT = 10000;
    private static final int BOUND = 1000000;

    @RepeatedTest(10)
    public void test() {
        String prefix = "hello-";
        VarCharType varCharType = new VarCharType();
        FieldRef fieldRef = new FieldRef(0, "", varCharType);

        RangeBitmapFileIndex bitmapFileIndex = new RangeBitmapFileIndex(varCharType, new Options());
        FileIndexWriter writer = bitmapFileIndex.createWriter();

        Random random = new Random();
        List<Pair<Integer, BinaryString>> pairs = new ArrayList<>();
        for (int i = 0; i < ROW_COUNT; i++) {
            BinaryString value =
                    random.nextBoolean()
                            ? BinaryString.fromString(prefix + random.nextInt(BOUND))
                            : null;
            pairs.add(Pair.of(i, value));
            writer.writeRecord(value);
        }

        // build index
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(stream, 0, bytes.length);

        // test eq
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix + random.nextInt(BOUND));
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (Objects.equals(value, predicate)) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, predicate)).get())
                    .isEqualTo(bitmap);
        }

        // test eq but value not exists
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix);
            assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, predicate)).get())
                    .isEqualTo(new RoaringBitmap32());
        }

        // test neq
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix + random.nextInt(BOUND));
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && !Objects.equals(value, predicate)) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitNotEqual(fieldRef, predicate)).get())
                    .isEqualTo(bitmap);
        }

        // test lt
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix + random.nextInt(BOUND));
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && value.compareTo(predicate) < 0) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitLessThan(fieldRef, predicate)).get())
                    .isEqualTo(bitmap);
        }

        // test lte
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix + random.nextInt(BOUND));
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && value.compareTo(predicate) <= 0) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitLessOrEqual(fieldRef, predicate)).get())
                    .isEqualTo(bitmap);
        }

        // test gt
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix + random.nextInt(BOUND));
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && value.compareTo(predicate) > 0) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, predicate)).get())
                    .isEqualTo(bitmap);
        }

        // test gte
        for (int i = 0; i < 10; i++) {
            BinaryString predicate = BinaryString.fromString(prefix + random.nextInt(BOUND));
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && value.compareTo(predicate) >= 0) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, predicate)).get())
                    .isEqualTo(bitmap);
        }

        // test in
        for (int i = 0; i < 10; i++) {
            int size = random.nextInt(5) + 3;
            Set<Object> literals = new HashSet<>();
            for (int j = 0; j < size; j++) {
                literals.add(BinaryString.fromString(prefix + random.nextInt(BOUND)));
            }
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && literals.contains(value)) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(
                            ((BitmapIndexResult)
                                            reader.visitIn(fieldRef, new ArrayList<>(literals)))
                                    .get())
                    .isEqualTo(bitmap);
        }

        // test not in
        for (int i = 0; i < 10; i++) {
            int size = random.nextInt(5) + 3;
            Set<Object> literals = new HashSet<>();
            for (int j = 0; j < size; j++) {
                literals.add(BinaryString.fromString(prefix + random.nextInt(BOUND)));
            }
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null && !literals.contains(value)) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(
                            ((BitmapIndexResult)
                                            reader.visitNotIn(fieldRef, new ArrayList<>(literals)))
                                    .get())
                    .isEqualTo(bitmap);
        }

        // test is null
        {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value == null) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get()).isEqualTo(bitmap);
        }

        // test is not null
        {
            RoaringBitmap32 bitmap = new RoaringBitmap32();
            for (Pair<Integer, BinaryString> pair : pairs) {
                BinaryString value = pair.getValue();
                if (value != null) {
                    bitmap.add(pair.getKey());
                }
            }
            assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                    .isEqualTo(bitmap);
        }
    }

    @Test
    public void testSimple() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        RangeBitmapFileIndex bitmapFileIndex = new RangeBitmapFileIndex(intType, new Options());
        FileIndexWriter writer = bitmapFileIndex.createWriter();
        writer.writeRecord(1);
        writer.writeRecord(3);
        writer.writeRecord(5);
        writer.writeRecord(7);
        writer.writeRecord(9);

        // build index
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(stream, 0, bytes.length);

        // test eq
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0));
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 2)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 3)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1));
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 4)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 5)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(2));

        // test gt
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 2, 3, 4));
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 2, 3, 4));
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 6)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4));
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 9)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // test gte
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 2, 3, 4));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 1)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1, 2, 3, 4));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 6)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(3, 4));
        assertThat(((BitmapIndexResult) reader.visitGreaterOrEqual(fieldRef, 9)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(4));
    }

    @Test
    public void testOnlyNulls() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        RangeBitmapFileIndex bitmapFileIndex = new RangeBitmapFileIndex(intType, new Options());
        FileIndexWriter writer = bitmapFileIndex.createWriter();
        writer.writeRecord(null);
        writer.writeRecord(null);

        // build index
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(stream, 0, bytes.length);

        // test is null
        assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf(0, 1));
        // test is not null
        assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // test EQ
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        // test GT
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
    }

    @Test
    public void testBuildEmpty() {
        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        RangeBitmapFileIndex bitmapFileIndex = new RangeBitmapFileIndex(intType, new Options());
        FileIndexWriter writer = bitmapFileIndex.createWriter();

        // build index
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream stream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(stream, 0, bytes.length);

        // test is null
        assertThat(((BitmapIndexResult) reader.visitIsNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        // test is not null
        assertThat(((BitmapIndexResult) reader.visitIsNotNull(fieldRef)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());

        // test EQ
        assertThat(((BitmapIndexResult) reader.visitEqual(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
        // test GT
        assertThat(((BitmapIndexResult) reader.visitGreaterThan(fieldRef, 0)).get())
                .isEqualTo(RoaringBitmap32.bitmapOf());
    }
}

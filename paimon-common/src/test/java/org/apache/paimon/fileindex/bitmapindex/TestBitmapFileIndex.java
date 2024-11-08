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

package org.apache.paimon.fileindex.bitmapindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResultLazy;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** test for {@link BitmapFileIndex}. */
public class TestBitmapFileIndex {

    @Test
    public void testFlip() {
        RoaringBitmap32 bitmap = RoaringBitmap32.bitmapOf(1, 3, 5);
        bitmap.flip(0, 6);
        assert bitmap.equals(RoaringBitmap32.bitmapOf(0, 2, 4));
    }

    @Test
    public void testBitmapIndex1() {
        VarCharType dataType = new VarCharType();
        FieldRef fieldRef = new FieldRef(0, "", dataType);
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(dataType, null);
        FileIndexWriter writer = bitmapFileIndex.createWriter();
        Object[] arr = {
            BinaryString.fromString("a"),
            null,
            BinaryString.fromString("b"),
            null,
            BinaryString.fromString("a"),
        };
        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream seekableStream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(seekableStream, 0, bytes.length);

        BitmapIndexResultLazy result1 =
                (BitmapIndexResultLazy) reader.visitEqual(fieldRef, BinaryString.fromString("a"));
        assert result1.get().equals(RoaringBitmap32.bitmapOf(0, 4));

        BitmapIndexResultLazy result2 =
                (BitmapIndexResultLazy) reader.visitEqual(fieldRef, BinaryString.fromString("b"));
        assert result2.get().equals(RoaringBitmap32.bitmapOf(2));

        BitmapIndexResultLazy result3 = (BitmapIndexResultLazy) reader.visitIsNull(fieldRef);
        assert result3.get().equals(RoaringBitmap32.bitmapOf(1, 3));

        BitmapIndexResultLazy result4 = (BitmapIndexResultLazy) result1.and(result2);
        assert result4.get().equals(RoaringBitmap32.bitmapOf());

        BitmapIndexResultLazy result5 = (BitmapIndexResultLazy) result1.or(result2);
        assert result5.get().equals(RoaringBitmap32.bitmapOf(0, 2, 4));
    }

    @Test
    public void testBitmapIndex2() {
        IntType dataType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", dataType);
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(dataType, null);
        FileIndexWriter writer = bitmapFileIndex.createWriter();
        Object[] arr = {0, 1, null};
        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream seekableStream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(seekableStream, 0, bytes.length);

        BitmapIndexResultLazy result1 = (BitmapIndexResultLazy) reader.visitEqual(fieldRef, 1);
        assert result1.get().equals(RoaringBitmap32.bitmapOf(1));

        BitmapIndexResultLazy result2 = (BitmapIndexResultLazy) reader.visitIsNull(fieldRef);
        assert result2.get().equals(RoaringBitmap32.bitmapOf(2));

        BitmapIndexResultLazy result3 = (BitmapIndexResultLazy) reader.visitIsNotNull(fieldRef);
        assert result3.get().equals(RoaringBitmap32.bitmapOf(0, 1));

        BitmapIndexResultLazy result4 =
                (BitmapIndexResultLazy) reader.visitNotIn(fieldRef, Arrays.asList(1, 2));
        assert result4.get().equals(RoaringBitmap32.bitmapOf(0, 2));

        BitmapIndexResultLazy result5 =
                (BitmapIndexResultLazy) reader.visitNotIn(fieldRef, Arrays.asList(1, 0));
        assert result5.get().equals(RoaringBitmap32.bitmapOf(2));
    }

    @Test
    public void testBitmapIndex3() {

        IntType intType = new IntType();
        FieldRef fieldRef = new FieldRef(0, "", intType);
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(intType, null);
        FileIndexWriter writer = bitmapFileIndex.createWriter();

        // test only one null-value
        Object[] arr = {1, 2, 1, 2, 1, 3, null};

        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream seekableStream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(seekableStream, 0, bytes.length);

        BitmapIndexResultLazy result1 = (BitmapIndexResultLazy) reader.visitEqual(fieldRef, 1);
        assert result1.get().equals(RoaringBitmap32.bitmapOf(0, 2, 4));

        // test read singleton bitmap
        BitmapIndexResultLazy result2 = (BitmapIndexResultLazy) reader.visitIsNull(fieldRef);
        assert result2.get().equals(RoaringBitmap32.bitmapOf(6));
    }
}

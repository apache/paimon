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
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexMetaV2;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.function.Consumer;

/** test for {@link BitmapFileIndex}. */
public class TestBitmapFileIndex {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testFlip() {
        RoaringBitmap32 bitmap = RoaringBitmap32.bitmapOf(1, 3, 5);
        bitmap.flip(0, 6);
        assert bitmap.equals(RoaringBitmap32.bitmapOf(0, 2, 4));
    }

    @Test
    public void testComparator() {
        assert BitmapFileIndexMetaV2.getComparator(new VarCharType())
                        .compare(BinaryString.fromString("a"), BinaryString.fromString("b"))
                < 0;
        assert BitmapFileIndexMetaV2.getComparator(new VarCharType())
                        .compare(BinaryString.fromString("a"), BinaryString.fromString("a"))
                == 0;
        assert BitmapFileIndexMetaV2.getComparator(new VarCharType())
                        .compare(BinaryString.fromString("c"), BinaryString.fromString("b"))
                > 0;
        assert BitmapFileIndexMetaV2.getComparator(new IntType()).compare(1, 2) < 0;
        assert BitmapFileIndexMetaV2.getComparator(new IntType()).compare(2, 1) > 0;
    }

    @Test
    public void testV1() throws Exception {
        testIntType(BitmapFileIndex.VERSION_1);
        testStringType(BitmapFileIndex.VERSION_1);
    }

    @Test
    public void testV2() throws Exception {
        testIntType(BitmapFileIndex.VERSION_2);
        testStringType(BitmapFileIndex.VERSION_2);
        testHighCardinality(BitmapFileIndex.VERSION_2, 1000000, 100000, null);
    }

    private FileIndexReader createTestReaderOnWriter(
            int writerVersion,
            Integer indexBlockSize,
            DataType dataType,
            Consumer<FileIndexWriter> consumer)
            throws Exception {
        Options options = new Options();
        options.setInteger(BitmapFileIndex.VERSION, writerVersion);
        if (indexBlockSize != null) {
            options.setInteger(BitmapFileIndex.INDEX_BLOCK_SIZE, indexBlockSize);
        }
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(dataType, options);
        FileIndexWriter writer;
        writer = bitmapFileIndex.createWriter();
        consumer.accept(writer);
        folder.create();
        File file = folder.newFile("f1");
        byte[] data = writer.serializedBytes();
        FileUtils.writeByteArrayToFile(file, data);
        LocalFileIO.LocalSeekableInputStream localSeekableInputStream =
                new LocalFileIO.LocalSeekableInputStream(file);
        return bitmapFileIndex.createReader(localSeekableInputStream, 0, 0);
    }

    private void testStringType(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
        BinaryString a = BinaryString.fromString("a");
        BinaryString b = BinaryString.fromString("b");
        Object[] dataColumn = {a, null, b, null, a};
        FileIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.STRING(),
                        writer -> {
                            for (Object o : dataColumn) {
                                writer.write(o);
                            }
                        });
        assert ((BitmapIndexResult) reader.visitEqual(fieldRef, a))
                .get()
                .equals(RoaringBitmap32.bitmapOf(0, 4));
        assert ((BitmapIndexResult) reader.visitEqual(fieldRef, b))
                .get()
                .equals(RoaringBitmap32.bitmapOf(2));
        assert ((BitmapIndexResult) reader.visitIsNull(fieldRef))
                .get()
                .equals(RoaringBitmap32.bitmapOf(1, 3));
        assert ((BitmapIndexResult) reader.visitIn(fieldRef, Arrays.asList(a, b)))
                .get()
                .equals(RoaringBitmap32.bitmapOf(0, 2, 4));
    }

    private void testIntType(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.INT());
        Object[] dataColumn = {0, 1, null};
        FileIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.INT(),
                        writer -> {
                            for (Object o : dataColumn) {
                                writer.write(o);
                            }
                        });
        assert ((BitmapIndexResult) reader.visitEqual(fieldRef, 0))
                .get()
                .equals(RoaringBitmap32.bitmapOf(0));
        assert ((BitmapIndexResult) reader.visitEqual(fieldRef, 1))
                .get()
                .equals(RoaringBitmap32.bitmapOf(1));
        assert ((BitmapIndexResult) reader.visitIsNull(fieldRef))
                .get()
                .equals(RoaringBitmap32.bitmapOf(2));
        assert ((BitmapIndexResult) reader.visitIn(fieldRef, Arrays.asList(0, 1, 2)))
                .get()
                .equals(RoaringBitmap32.bitmapOf(0, 1));
    }

    private void testHighCardinality(
            int version, int rowCount, int approxCardinality, Integer secondaryBlockSize)
            throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
        RoaringBitmap32 middleBm = new RoaringBitmap32();
        RoaringBitmap32 nullBm = new RoaringBitmap32();
        long time1 = System.currentTimeMillis();
        String prefix = "ssssssssss";
        FileIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        secondaryBlockSize,
                        DataTypes.STRING(),
                        writer -> {
                            for (int i = 0; i < rowCount; i++) {

                                int sid = (int) (Math.random() * approxCardinality);
                                if (sid == approxCardinality / 2) {
                                    middleBm.add(i);
                                } else if (Math.random() < 0.01) {
                                    nullBm.add(i);
                                    writer.write(null);
                                    continue;
                                }
                                writer.write(BinaryString.fromString(prefix + sid));
                            }
                        });
        System.out.println("write time: " + (System.currentTimeMillis() - time1));
        long time2 = System.currentTimeMillis();
        FileIndexResult result =
                reader.visitEqual(
                        fieldRef, BinaryString.fromString(prefix + (approxCardinality / 2)));
        RoaringBitmap32 resultBm = ((BitmapIndexResult) result).get();
        System.out.println("read time: " + (System.currentTimeMillis() - time2));
        assert resultBm.equals(middleBm);
        long time3 = System.currentTimeMillis();
        FileIndexResult resultNull = reader.visitIsNull(fieldRef);
        RoaringBitmap32 resultNullBm = ((BitmapIndexResult) resultNull).get();
        System.out.println("read null bitmap time: " + (System.currentTimeMillis() - time3));
        assert resultNullBm.equals(nullBm);
    }

    @Test
    void testBitmapIndexForBooleanType() {

        BooleanType booleanType = new BooleanType();
        FieldRef fieldRef = new FieldRef(0, "", booleanType);
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(booleanType, null);
        FileIndexWriter writer = bitmapFileIndex.createWriter();

        Object[] arr = {Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE, null};

        for (Object o : arr) {
            writer.write(o);
        }
        byte[] bytes = writer.serializedBytes();
        ByteArraySeekableStream seekableStream = new ByteArraySeekableStream(bytes);
        FileIndexReader reader = bitmapFileIndex.createReader(seekableStream, 0, bytes.length);

        BitmapIndexResult searchTrueResult =
                (BitmapIndexResult) reader.visitEqual(fieldRef, Boolean.TRUE);
        Assertions.assertThat(searchTrueResult.get()).isEqualTo(RoaringBitmap32.bitmapOf(0, 2));

        BitmapIndexResult searchNullResult = (BitmapIndexResult) reader.visitIsNull(fieldRef);
        Assertions.assertThat(searchNullResult.get()).isEqualTo(RoaringBitmap32.bitmapOf(4));
    }
}

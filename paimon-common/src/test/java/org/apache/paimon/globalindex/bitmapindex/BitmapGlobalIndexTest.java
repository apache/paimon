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

package org.apache.paimon.globalindex.bitmapindex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndex;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

/** Tests for {@link BitmapGlobalIndex}. */
public class BitmapGlobalIndexTest {

    @TempDir private File tempDir;

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testV1() throws Exception {
        testIntType(BitmapFileIndex.VERSION_1);
        testStringType(BitmapFileIndex.VERSION_1);
        testBooleanType(BitmapFileIndex.VERSION_1);
        testHighCardinality(BitmapFileIndex.VERSION_1, 1000000, 100000, null);
        testStringTypeWithReusing(BitmapFileIndex.VERSION_1);
        testAllNull(BitmapFileIndex.VERSION_1);
    }

    @Test
    public void testV2() throws Exception {
        testIntType(BitmapFileIndex.VERSION_2);
        testStringType(BitmapFileIndex.VERSION_2);
        testBooleanType(BitmapFileIndex.VERSION_2);
        testHighCardinality(BitmapFileIndex.VERSION_2, 1000000, 100000, null);
        testStringTypeWithReusing(BitmapFileIndex.VERSION_2);
        testAllNull(BitmapFileIndex.VERSION_2);
    }

    private void testStringType(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
        BinaryString a = BinaryString.fromString("a");
        BinaryString b = BinaryString.fromString("b");
        Object[] dataColumn = {a, null, b, null, a};
        GlobalIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.STRING(),
                        writer -> {
                            for (Object o : dataColumn) {
                                writer.write(o);
                            }
                        });
        assert reader.visitEqual(fieldRef, a)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0, 4));
        assert reader.visitEqual(fieldRef, b)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(2));
        assert reader.visitIsNull(fieldRef)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(1, 3));
        assert reader.visitIn(fieldRef, Arrays.asList(a, b))
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0, 2, 4));
        assert reader.visitEqual(fieldRef, BinaryString.fromString("c")).get().results().isEmpty();
    }

    private void testIntType(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.INT());
        Object[] dataColumn = {0, 1, null};
        GlobalIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.INT(),
                        writer -> {
                            for (Object o : dataColumn) {
                                writer.write(o);
                            }
                        });
        assert reader.visitEqual(fieldRef, 0)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0));
        assert reader.visitEqual(fieldRef, 1)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(1));
        assert reader.visitIsNull(fieldRef)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(2));
        assert reader.visitIn(fieldRef, Arrays.asList(0, 1, 2))
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0, 1));

        assert reader.visitEqual(fieldRef, 2).get().results().isEmpty();
    }

    private void testBooleanType(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.BOOLEAN());
        Object[] dataColumn = {Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE, null};
        GlobalIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.BOOLEAN(),
                        writer -> {
                            for (Object o : dataColumn) {
                                writer.write(o);
                            }
                        });
        assert reader.visitEqual(fieldRef, Boolean.TRUE)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0, 2));
        assert reader.visitIsNull(fieldRef)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(4));
    }

    private void testHighCardinality(
            int version, int rowCount, int approxCardinality, Integer secondaryBlockSize)
            throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
        RoaringBitmap32 middleBm = new RoaringBitmap32();
        RoaringBitmap32 nullBm = new RoaringBitmap32();
        long time1 = System.currentTimeMillis();
        String prefix = "ssssssssss";
        GlobalIndexReader reader =
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
        GlobalIndexResult result =
                reader.visitEqual(
                                fieldRef, BinaryString.fromString(prefix + (approxCardinality / 2)))
                        .get();
        System.out.println("read time: " + (System.currentTimeMillis() - time2));
        assert result.results().equals(middleBm.toNavigable64());
        long time3 = System.currentTimeMillis();
        GlobalIndexResult resultNull = reader.visitIsNull(fieldRef).get();
        System.out.println("read null bitmap time: " + (System.currentTimeMillis() - time3));
        assert resultNull.results().equals(nullBm.toNavigable64());
    }

    private GlobalIndexReader createTestReaderOnWriter(
            int writerVersion,
            Integer indexBlockSize,
            DataType dataType,
            Consumer<GlobalIndexSingletonWriter> consumer)
            throws Exception {
        Options options = new Options();
        options.setInteger(BitmapFileIndex.VERSION, writerVersion);
        if (indexBlockSize != null) {
            options.setInteger(BitmapFileIndex.INDEX_BLOCK_SIZE, indexBlockSize);
        }
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(dataType, options);
        BitmapGlobalIndex bitmapGlobalIndex = new BitmapGlobalIndex(bitmapFileIndex);
        final FileIO fileIO = new LocalFileIO();
        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(new Path(tempDir.toString(), fileName), true);
                    }
                };
        GlobalIndexSingletonWriter globalIndexWriter = bitmapGlobalIndex.createWriter(fileWriter);
        consumer.accept(globalIndexWriter);
        String fileName = globalIndexWriter.finish().get(0).fileName();
        Path path = new Path(tempDir.toString(), fileName);
        long fileSize = fileIO.getFileSize(path);

        GlobalIndexFileReader fileReader =
                new GlobalIndexFileReader() {
                    @Override
                    public SeekableInputStream getInputStream(String fileName) throws IOException {
                        return fileIO.newInputStream(new Path(tempDir.toString(), fileName));
                    }

                    @Override
                    public Path filePath(String fileName) {
                        return new Path(tempDir.toString(), fileName);
                    }
                };

        GlobalIndexIOMeta globalIndexMeta = new GlobalIndexIOMeta(fileName, fileSize, null);

        return bitmapGlobalIndex.createReader(
                fileReader, Collections.singletonList(globalIndexMeta));
    }

    private void testStringTypeWithReusing(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
        BinaryString a = BinaryString.fromString("a");
        BinaryString b = BinaryString.fromString("b");
        BinaryString c = BinaryString.fromString("a");
        GlobalIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.STRING(),
                        writer -> {
                            writer.write(a);
                            writer.write(null);
                            a.pointTo(b.getSegments(), b.getOffset(), b.getSizeInBytes());
                            writer.write(null);
                            writer.write(a);
                            writer.write(null);
                            a.pointTo(c.getSegments(), c.getOffset(), c.getSizeInBytes());
                            writer.write(null);
                        });
        assert reader.visitEqual(fieldRef, a)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0));
        assert reader.visitEqual(fieldRef, b)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(3));
        assert reader.visitIsNull(fieldRef)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(1, 2, 4, 5));
        assert reader.visitIn(fieldRef, Arrays.asList(a, b))
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0, 3));
        assert reader.visitEqual(fieldRef, BinaryString.fromString("c")).get().results().isEmpty();
    }

    private void testAllNull(int version) throws Exception {
        FieldRef fieldRef = new FieldRef(0, "", DataTypes.INT());
        Object[] dataColumn = {null, null, null};
        GlobalIndexReader reader =
                createTestReaderOnWriter(
                        version,
                        null,
                        DataTypes.INT(),
                        writer -> {
                            for (Object o : dataColumn) {
                                writer.write(o);
                            }
                        });
        assert reader.visitIsNull(fieldRef)
                .get()
                .results()
                .equals(RoaringNavigableMap64.bitmapOf(0, 1, 2));
        assert reader.visitIsNotNull(fieldRef).get().results().isEmpty();
    }
}

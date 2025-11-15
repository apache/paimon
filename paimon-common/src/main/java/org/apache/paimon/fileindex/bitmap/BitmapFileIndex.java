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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/** The implementation of bitmap file index. */
public class BitmapFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

    public static final String VERSION = "version";
    public static final String INDEX_BLOCK_SIZE = "index-block-size";

    private final DataType dataType;
    private final Options options;

    public BitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, options);
    }

    @Override
    public FileIndexReader createReader(
            SeekableInputStream seekableInputStream, int start, int length) {
        try {
            return new Reader(seekableInputStream, start, options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class Writer extends FileIndexWriter {

        private final int version;
        private final BitmapWriterHelper<RoaringBitmap32> helper;
        private final RoaringBitmap32 nullBitmap = new RoaringBitmap32();
        private int rowNumber;

        public Writer(DataType dataType, Options options) {
            this.version = options.getInteger(VERSION, VERSION_2);
            this.helper =
                    new BitmapWriterHelper<>(
                            version,
                            dataType,
                            options,
                            new BitmapWriterHelper.Bitmap32Operations());
        }

        @Override
        public void write(Object key) {
            if (key == null) {
                nullBitmap.add(rowNumber++);
            } else {
                helper.add(key, rowNumber++);
            }
        }

        @Override
        public byte[] serializedBytes() {
            try {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(output);

                byte[] nullBitmapBytes = nullBitmap.serialize();
                int nullBitmapLength =
                        nullBitmap.isEmpty() || nullBitmap.getCardinality() == 1
                                ? 0
                                : nullBitmapBytes.length;
                int nullOffset =
                        nullBitmap.getCardinality() == 1 ? -1 - nullBitmap.iterator().next() : 0;

                // Special handling for VERSION_1
                if (version == VERSION_1) {
                    dos.writeByte(version);
                    BitmapFileIndexMeta bitmapFileIndexMeta =
                            new BitmapFileIndexMeta(
                                    helper.dataType,
                                    helper.options,
                                    rowNumber,
                                    helper.getId2bitmap().size(),
                                    !nullBitmap.isEmpty(),
                                    nullOffset,
                                    buildBitmapOffsets(nullBitmapLength));
                    bitmapFileIndexMeta.serialize(dos);
                    writeBody(dos, nullBitmapBytes, nullBitmapLength);
                } else {
                    helper.serialize(
                            dos,
                            nullBitmapLength > 0 ? nullBitmapBytes : null,
                            nullBitmapLength,
                            rowNumber,
                            !nullBitmap.isEmpty(),
                            nullOffset);
                }

                return output.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private LinkedHashMap<Object, Integer> buildBitmapOffsets(int nullBitmapLength) {
            LinkedHashMap<Object, Integer> bitmapOffsets = new LinkedHashMap<>();
            int[] offsetRef = {nullBitmapLength};
            helper.getId2bitmap()
                    .forEach(
                            (k, v) -> {
                                if (v.getCardinality() == 1) {
                                    bitmapOffsets.put(k, -1 - v.iterator().next());
                                } else {
                                    byte[] bytes = v.serialize();
                                    bitmapOffsets.put(k, offsetRef[0]);
                                    offsetRef[0] += bytes.length;
                                }
                            });
            return bitmapOffsets;
        }

        private void writeBody(DataOutputStream dos, byte[] nullBitmapBytes, int nullBitmapLength)
                throws Exception {
            if (nullBitmapLength > 0) {
                dos.write(nullBitmapBytes);
            }
            for (RoaringBitmap32 bitmap : helper.getId2bitmap().values()) {
                if (bitmap.getCardinality() > 1) {
                    dos.write(bitmap.serialize());
                }
            }
        }
    }

    private static class Reader extends FileIndexReader {

        private final BitmapReaderHelper<RoaringBitmap32> helper;

        public Reader(SeekableInputStream seekableInputStream, int start, Options options) {
            this.helper =
                    new BitmapReaderHelper<>(
                            seekableInputStream,
                            start,
                            options,
                            new BitmapReaderHelper.Bitmap32Operations());
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return visitIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return visitNotIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        helper.readInternalMeta(fieldRef.type(), this::createBitmapFileIndexMeta);
                        return helper.getInListResultBitmap(literals);
                    });
        }

        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        helper.readInternalMeta(fieldRef.type(), this::createBitmapFileIndexMeta);
                        RoaringBitmap32 bitmap = helper.getInListResultBitmap(literals);
                        bitmap.flip(0, helper.getBitmapFileIndexMeta().getRowCount());
                        return bitmap;
                    });
        }

        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return visitIn(fieldRef, Collections.singletonList(null));
        }

        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return visitNotIn(fieldRef, Collections.singletonList(null));
        }

        private BitmapFileIndexMeta createBitmapFileIndexMeta(
                int version, DataType dataType, Options options) {
            if (version == VERSION_1) {
                return new BitmapFileIndexMeta(dataType, options);
            } else if (version == VERSION_2) {
                return new BitmapFileIndexMetaV2(dataType, options);
            } else if (version > VERSION_2) {
                throw new RuntimeException(
                        String.format(
                                "read index file fail, your plugin version is lower than %d",
                                version));
            }
            throw new RuntimeException("Invalid version: " + version);
        }
    }
}

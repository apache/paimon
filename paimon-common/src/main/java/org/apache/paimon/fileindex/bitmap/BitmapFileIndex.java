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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** implementation of bitmap file index. */
public class BitmapFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;

    private final DataType dataType;

    public BitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType);
    }

    @Override
    public FileIndexReader createReader(
            SeekableInputStream seekableInputStream, int start, int length) {
        try {
            return new Reader(seekableInputStream, start, length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class Writer extends FileIndexWriter {

        private final DataType dataType;
        private final Function<Object, Object> valueMapper;
        private final Map<Object, RoaringBitmap32> id2bitmap = new HashMap<>();
        private final RoaringBitmap32 nullBitmap = new RoaringBitmap32();
        private int rowNumber;

        public Writer(DataType dataType) {
            this.dataType = dataType;
            this.valueMapper = getValueMapper(dataType);
        }

        @Override
        public void write(Object key) {
            if (key == null) {
                nullBitmap.add(rowNumber++);
            } else {
                id2bitmap
                        .computeIfAbsent(valueMapper.apply(key), k -> new RoaringBitmap32())
                        .add(rowNumber++);
            }
        }

        @Override
        public byte[] serializedBytes() {

            try {

                ByteArrayOutputStream output = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(output);

                dos.writeByte(VERSION_1);

                // 1.serialize bitmaps to bytes
                byte[] nullBitmapBytes = nullBitmap.serialize();
                Map<Object, byte[]> id2bitmapBytes =
                        id2bitmap.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> e.getKey(), e -> e.getValue().serialize()));

                // 2.build bitmap file index meta
                LinkedHashMap<Object, Integer> bitmapOffsets = new LinkedHashMap<>();
                LinkedList<byte[]> serializeBitmaps = new LinkedList<>();
                int[] offsetRef = {
                    nullBitmap.isEmpty() || nullBitmap.getCardinality() == 1
                            ? 0
                            : nullBitmapBytes.length
                };
                id2bitmap.forEach(
                        (k, v) -> {
                            if (v.getCardinality() == 1) {
                                bitmapOffsets.put(k, -1 - v.iterator().next());
                            } else {
                                byte[] bytes = id2bitmapBytes.get(k);
                                serializeBitmaps.add(bytes);
                                bitmapOffsets.put(k, offsetRef[0]);
                                offsetRef[0] += bytes.length;
                            }
                        });
                BitmapFileIndexMeta bitmapFileIndexMeta =
                        new BitmapFileIndexMeta(
                                dataType,
                                rowNumber,
                                id2bitmap.size(),
                                !nullBitmap.isEmpty(),
                                nullBitmap.getCardinality() == 1
                                        ? -1 - nullBitmap.iterator().next()
                                        : 0,
                                bitmapOffsets);

                // 3.serialize meta
                bitmapFileIndexMeta.serialize(dos);

                // 4.serialize body
                if (nullBitmap.getCardinality() > 1) {
                    dos.write(nullBitmapBytes);
                }
                for (byte[] bytes : serializeBitmaps) {
                    dos.write(bytes);
                }
                return output.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class Reader extends FileIndexReader {

        private final SeekableInputStream seekableInputStream;
        private final int headStart;
        private int bodyStart;
        private final Map<Object, RoaringBitmap32> bitmaps = new LinkedHashMap<>();

        private int version;
        private BitmapFileIndexMeta bitmapFileIndexMeta;
        private Function<Object, Object> valueMapper;

        public Reader(SeekableInputStream seekableInputStream, int start, int length) {
            this.seekableInputStream = seekableInputStream;
            this.headStart = start;
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
                        readInternalMeta(fieldRef.type());
                        return getInListResultBitmap(literals);
                    });
        }

        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        readInternalMeta(fieldRef.type());
                        RoaringBitmap32 bitmap = getInListResultBitmap(literals);
                        bitmap.flip(0, bitmapFileIndexMeta.getRowCount());
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

        private RoaringBitmap32 getInListResultBitmap(List<Object> literals) {
            return RoaringBitmap32.or(
                    literals.stream()
                            .map(
                                    it ->
                                            bitmaps.computeIfAbsent(
                                                    valueMapper.apply(it), k -> readBitmap(k)))
                            .iterator());
        }

        private RoaringBitmap32 readBitmap(Object bitmapId) {
            try {
                if (!bitmapFileIndexMeta.contains(bitmapId)) {
                    return new RoaringBitmap32();
                } else {
                    int offset = bitmapFileIndexMeta.getOffset(bitmapId);
                    if (offset < 0) {
                        return RoaringBitmap32.bitmapOf(-1 - offset);
                    } else {
                        seekableInputStream.seek(bodyStart + offset);
                        RoaringBitmap32 bitmap = new RoaringBitmap32();
                        bitmap.deserialize(new DataInputStream(seekableInputStream));
                        return bitmap;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void readInternalMeta(DataType dataType) {
            if (this.bitmapFileIndexMeta == null) {
                this.valueMapper = getValueMapper(dataType);
                try {
                    seekableInputStream.seek(headStart);
                    this.version = seekableInputStream.read();
                    if (this.version > VERSION_1) {
                        throw new RuntimeException(
                                String.format(
                                        "read index file fail, "
                                                + "your plugin version is lower than %d",
                                        this.version));
                    }
                    DataInput input = new DataInputStream(seekableInputStream);
                    this.bitmapFileIndexMeta = new BitmapFileIndexMeta(dataType);
                    this.bitmapFileIndexMeta.deserialize(input);
                    bodyStart = (int) seekableInputStream.getPos();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // Currently, it is mainly used to convert timestamps to long
    public static Function<Object, Object> getValueMapper(DataType dataType) {
        return dataType.accept(
                new DataTypeDefaultVisitor<Function<Object, Object>>() {
                    @Override
                    public Function<Object, Object> visit(TimestampType timestampType) {
                        return getTimeStampMapper(timestampType.getPrecision());
                    }

                    @Override
                    public Function<Object, Object> visit(
                            LocalZonedTimestampType localZonedTimestampType) {
                        return getTimeStampMapper(localZonedTimestampType.getPrecision());
                    }

                    @Override
                    protected Function<Object, Object> defaultMethod(DataType dataType) {
                        return Function.identity();
                    }

                    private Function<Object, Object> getTimeStampMapper(int precision) {
                        return o -> {
                            if (o == null) {
                                return null;
                            } else if (precision <= 3) {
                                return ((Timestamp) o).getMillisecond();
                            } else {
                                return ((Timestamp) o).toMicros();
                            }
                        };
                    }
                });
    }
}

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

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.KeyFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;

/** Implementation of range-bitmap file index. */
public class RangeBitmapFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;
    public static final int CURRENT_VERSION = VERSION_1;

    private final DataType dataType;
    private final Options options;
    public static final String CHUNK_SIZE = "chunk-size";

    public RangeBitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, options);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream in, int start, int length) {
        return new Reader(dataType, in, start);
    }

    private static class Writer extends FileIndexWriter {

        private final Function<Object, Object> converter;
        private final RangeBitmap.Appender appender;

        public Writer(DataType dataType, Options options) {
            KeyFactory factory = KeyFactory.create(dataType);
            String chunkSize = options.getString(CHUNK_SIZE, factory.defaultChunkSize());
            this.converter = factory.createConverter();
            this.appender =
                    new RangeBitmap.Appender(factory, (int) MemorySize.parse(chunkSize).getBytes());
        }

        @Override
        public void write(Object key) {
            appender.append(converter.apply(key));
        }

        @Override
        public byte[] serializedBytes() {
            return appender.serialize();
        }
    }

    private static class Reader extends FileIndexReader {

        private final Function<Object, Object> converter;
        private final RangeBitmap bitmap;

        public Reader(DataType dataType, SeekableInputStream in, int start) {
            KeyFactory factory = KeyFactory.create(dataType);
            this.converter = factory.createConverter();
            this.bitmap = new RangeBitmap(in, start, factory);
        }

        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return new BitmapIndexResult(bitmap::isNull);
        }

        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return new BitmapIndexResult(bitmap::isNotNull);
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.eq(converter.apply(literal)));
        }

        @Override
        public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.neq(converter.apply(literal)));
        }

        @Override
        public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> bitmap.in(literals.stream().map(converter).collect(Collectors.toList())));
        }

        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () ->
                            bitmap.notIn(
                                    literals.stream().map(converter).collect(Collectors.toList())));
        }

        @Override
        public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.lt(converter.apply(literal)));
        }

        @Override
        public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.lte(converter.apply(literal)));
        }

        @Override
        public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.gt(converter.apply(literal)));
        }

        @Override
        public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.gte(converter.apply(literal)));
        }

        @Override
        public FileIndexResult visitBetween(FieldRef fieldRef, Object from, Object to) {
            return new BitmapIndexResult(
                    () -> {
                        RoaringBitmap32 gte = bitmap.gte(converter.apply(from));
                        RoaringBitmap32 lte = bitmap.lte(converter.apply(to));
                        return RoaringBitmap32.and(gte, lte);
                    });
        }

        @Override
        public FileIndexResult visitTopN(TopN topN, FileIndexResult result) {
            RoaringBitmap32 foundSet =
                    result instanceof BitmapIndexResult ? ((BitmapIndexResult) result).get() : null;

            int limit = topN.limit();
            List<SortValue> orders = topN.orders();
            SortValue sort = orders.get(0);
            SortValue.NullOrdering nullOrdering = sort.nullOrdering();
            boolean strict = orders.size() == 1;
            if (ASCENDING.equals(sort.direction())) {
                return new BitmapIndexResult(
                        () -> bitmap.bottomK(limit, nullOrdering, foundSet, strict));
            } else {
                return new BitmapIndexResult(
                        () -> bitmap.topK(limit, nullOrdering, foundSet, strict));
            }
        }
    }
}

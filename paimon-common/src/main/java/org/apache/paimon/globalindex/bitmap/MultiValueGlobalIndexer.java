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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Bitmap-backed multivalue index over the elements of an array column. */
public class MultiValueGlobalIndexer implements SortedGlobalIndexer {

    private final DataType elementType;
    private final KeySerializer keySerializer;
    private final GlobalIndexKeyExtractor keyExtractor;
    private final int dictionaryBlockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;

    public MultiValueGlobalIndexer(DataField dataField, Options options) {
        Preconditions.checkArgument(
                dataField.type() instanceof ArrayType,
                "Multivalue index requires an ARRAY column, but column '%s' has type %s.",
                dataField.name(),
                dataField.type());
        this.elementType = ((ArrayType) dataField.type()).getElementType();
        this.keySerializer = KeySerializer.create(elementType);
        this.keyExtractor = new ArrayElementKeyExtractor(elementType);
        this.dictionaryBlockSize =
                (int) options.get(MultiValueGlobalIndexOptions.DICTIONARY_BLOCK_SIZE).getBytes();
        CompressOptions compressOptions =
                new CompressOptions(
                        options.get(MultiValueGlobalIndexOptions.COMPRESSION),
                        options.get(MultiValueGlobalIndexOptions.COMPRESSION_LEVEL));
        this.compressionFactory = BlockCompressionFactory.create(compressOptions);
    }

    @Override
    public GlobalIndexKeyExtractor keyExtractor() {
        return keyExtractor;
    }

    @Override
    public MultiValueBitmapIndexWriter createWriter(GlobalIndexFileWriter fileWriter)
            throws IOException {
        return new MultiValueBitmapIndexWriter(
                fileWriter, keySerializer, dictionaryBlockSize, compressionFactory);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            ExecutorService executor) {
        return new MultiValueBitmapIndexReader(
                fileReader, files, keySerializer, totalRowCount, executor);
    }

    private static class ArrayElementKeyExtractor implements GlobalIndexKeyExtractor {

        private static final long serialVersionUID = 1L;

        private final DataType elementType;
        private final InternalArray.ElementGetter elementGetter;

        private ArrayElementKeyExtractor(DataType elementType) {
            this.elementType = elementType;
            this.elementGetter = InternalArray.createElementGetter(elementType);
        }

        @Override
        public DataType keyType() {
            return elementType;
        }

        @Override
        public void extract(@Nullable Object sourceValue, KeyConsumer consumer) throws IOException {
            if (sourceValue == null) {
                return;
            }
            Preconditions.checkArgument(
                    sourceValue instanceof InternalArray,
                    "Multivalue index expects InternalArray values, but got %s.",
                    sourceValue.getClass().getName());
            InternalArray array = (InternalArray) sourceValue;
            for (int i = 0; i < array.size(); i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                if (element != null) {
                    consumer.accept(element);
                }
            }
        }
    }
}

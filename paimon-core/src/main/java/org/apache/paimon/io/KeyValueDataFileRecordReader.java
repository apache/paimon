/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.io;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link RecordReader} for reading {@link KeyValue} data files. */
public class KeyValueDataFileRecordReader implements RecordReader<KeyValue> {

    private final RecordReader<InternalRow> reader;
    private final KeyValueSerializer serializer;
    private final int level;
    @Nullable private final int[] indexMapping;
    @Nullable private final CastFieldGetter[] castMapping;

    public KeyValueDataFileRecordReader(
            FileIO fileIO,
            FormatReaderFactory readerFactory,
            Path path,
            RowType keyType,
            RowType valueType,
            int level,
            @Nullable Integer poolSize,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping)
            throws IOException {
        FileUtils.checkExists(fileIO, path);
        this.reader =
                poolSize == null
                        ? readerFactory.createReader(fileIO, path)
                        : readerFactory.createReader(fileIO, path, poolSize);
        this.serializer = new KeyValueSerializer(keyType, valueType);
        this.level = level;
        this.indexMapping = indexMapping;
        this.castMapping = castMapping;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
        return iterator == null
                ? null
                : new KeyValueDataFileRecordIterator(iterator, indexMapping, castMapping);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private class KeyValueDataFileRecordIterator extends AbstractFileRecordIterator<KeyValue> {

        private final RecordReader.RecordIterator<InternalRow> iterator;

        private KeyValueDataFileRecordIterator(
                RecordReader.RecordIterator<InternalRow> iterator,
                @Nullable int[] indexMapping,
                @Nullable CastFieldGetter[] castMapping) {
            super(indexMapping, castMapping);
            this.iterator = iterator;
        }

        @Override
        public KeyValue next() throws IOException {
            InternalRow result = iterator.next();

            if (result == null) {
                return null;
            } else {
                return serializer.fromRow(mappingRowData(result)).setLevel(level);
            }
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }
}

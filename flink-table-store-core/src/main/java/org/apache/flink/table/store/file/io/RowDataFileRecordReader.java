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

package org.apache.flink.table.store.file.io;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** Reads {@link RowData} from data files. */
public class RowDataFileRecordReader implements RecordReader<RowData> {

    private final BulkFormat.Reader<RowData> reader;
    @Nullable private final int[] indexMapping;

    public RowDataFileRecordReader(
            Path path,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            @Nullable int[] indexMapping)
            throws IOException {
        this.reader = FileUtils.createFormatReader(readerFactory, path);
        this.indexMapping = indexMapping;
    }

    @Nullable
    @Override
    public RecordReader.RecordIterator<RowData> readBatch() throws IOException {
        BulkFormat.RecordIterator<RowData> iterator = reader.readBatch();
        return iterator == null ? null : new RowDataFileRecordIterator(iterator, indexMapping);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static class RowDataFileRecordIterator extends AbstractFileRecordIterator<RowData> {

        private final BulkFormat.RecordIterator<RowData> iterator;

        private RowDataFileRecordIterator(
                BulkFormat.RecordIterator<RowData> iterator, @Nullable int[] indexMapping) {
            super(indexMapping);
            this.iterator = iterator;
        }

        @Override
        public RowData next() throws IOException {
            RecordAndPosition<RowData> result = iterator.next();

            return result == null ? null : mappingRowData(result.getRecord());
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }
}

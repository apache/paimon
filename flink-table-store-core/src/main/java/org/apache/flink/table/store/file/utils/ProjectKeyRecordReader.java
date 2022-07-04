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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.utils.ProjectedRowData;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link RecordReader} with key projection. */
public class ProjectKeyRecordReader implements RecordReader<KeyValue> {

    private final RecordReader<KeyValue> reader;
    private final ProjectedRowData projectedRow;

    public ProjectKeyRecordReader(RecordReader<KeyValue> reader, int[][] keyProjectedFields) {
        this.reader = reader;
        this.projectedRow = ProjectedRowData.from(keyProjectedFields);
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = reader.readBatch();
        if (batch == null) {
            return null;
        }

        return new ProjectedIterator(batch, projectedRow);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    /** A {@link RecordIterator} with key projection. */
    public static class ProjectedIterator implements RecordIterator<KeyValue> {

        private final RecordIterator<KeyValue> iterator;
        private final ProjectedRowData projectedRow;

        public ProjectedIterator(RecordIterator<KeyValue> iterator, ProjectedRowData projectedRow) {
            this.iterator = iterator;
            this.projectedRow = projectedRow;
        }

        @Override
        public KeyValue next() throws IOException {
            KeyValue kv = iterator.next();
            if (kv == null) {
                return null;
            }
            return kv.replaceKey(projectedRow.replaceRow(kv.key()));
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }
}

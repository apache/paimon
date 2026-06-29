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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link RecordReader} which apply {@link DeletionVector} to filter record. */
public class ApplyDeletionVectorReader implements FileRecordReader<InternalRow> {

    private final FileRecordReader<InternalRow> reader;

    private final DeletionVector deletionVector;

    /**
     * The file offset of this reader comparing to the deletion vector. This is useful in
     * data-evolution scenario, where a blob/vector file may only cover a partial range of the
     * deletion vector.
     */
    private final long fileOffset;

    public ApplyDeletionVectorReader(
            FileRecordReader<InternalRow> reader, DeletionVector deletionVector) {
        this(reader, deletionVector, 0L);
    }

    public ApplyDeletionVectorReader(
            FileRecordReader<InternalRow> reader, DeletionVector deletionVector, long fileOffset) {
        this.reader = reader;
        this.deletionVector = deletionVector;
        this.fileOffset = fileOffset;
    }

    public RecordReader<InternalRow> reader() {
        return reader;
    }

    public DeletionVector deletionVector() {
        return deletionVector;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        return new ApplyDeletionFileRecordIterator(batch, deletionVector, fileOffset);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

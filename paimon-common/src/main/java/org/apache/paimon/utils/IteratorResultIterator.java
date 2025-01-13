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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A simple {@link RecordReader.RecordIterator} that returns the elements of an iterator. */
public final class IteratorResultIterator extends RecyclableIterator<InternalRow>
        implements FileRecordIterator<InternalRow> {

    private final IteratorWithException<InternalRow, IOException> records;
    private final Path filePath;
    private long nextFilePos;

    public IteratorResultIterator(
            final IteratorWithException<InternalRow, IOException> records,
            final @Nullable Runnable recycler,
            final Path filePath,
            long pos) {
        super(recycler);
        this.records = records;
        this.filePath = filePath;
        this.nextFilePos = pos;
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        if (records.hasNext()) {
            nextFilePos++;
            return records.next();
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        return nextFilePos - 1;
    }

    @Override
    public Path filePath() {
        return filePath;
    }
}

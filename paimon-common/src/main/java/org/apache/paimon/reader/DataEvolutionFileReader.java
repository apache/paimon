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

package org.apache.paimon.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * This is a union reader which contains multiple inner readers.
 *
 * <pre> This reader, assembling multiple reader into one big and great reader. The row it produces
 * also come from the readers it contains.
 *
 * For example, the expected schema for this reader is : int, int, string, int, string, int.(Total 6 fields)
 * It contains three inner readers, we call them reader0, reader1 and reader2.
 *
 * The rowOffsets and fieldOffsets are all 6 elements long the same as
 * output schema. RowOffsets is used to indicate which inner reader the field comes from, and
 * fieldOffsets is used to indicate the offset of the field in the inner reader.
 *
 * For example, if rowOffsets is {0, 2, 0, 1, 2, 1} and fieldOffsets is {0, 0, 1, 1, 1, 0}, it means:
 * - The first field comes from reader0, and it is at offset 0 in reader1.
 * - The second field comes from reader2, and it is at offset 0 in reader3.
 * - The third field comes from reader0, and it is at offset 1 in reader1.
 * - The fourth field comes from reader1, and it is at offset 1 in reader2.
 * - The fifth field comes from reader2, and it is at offset 1 in reader3.
 * - The sixth field comes from reader1, and it is at offset 0 in reader2.
 *
 * These three readers work together, package out final and complete rows.
 *
 * </pre>
 */
public class DataEvolutionFileReader implements RecordReader<InternalRow> {

    private final int[] rowOffsets;
    private final int[] fieldOffsets;
    private final RecordReader<InternalRow>[] readers;

    public DataEvolutionFileReader(
            int[] rowOffsets, int[] fieldOffsets, RecordReader<InternalRow>[] readers) {
        checkArgument(rowOffsets != null, "Row offsets must not be null");
        checkArgument(fieldOffsets != null, "Field offsets must not be null");
        checkArgument(
                rowOffsets.length == fieldOffsets.length,
                "Row offsets and field offsets must have the same length");
        checkArgument(rowOffsets.length > 0, "Row offsets must not be empty");
        checkArgument(readers != null && readers.length > 1, "Readers should be more than 1");
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
        this.readers = readers;
    }

    @Override
    @Nullable
    public RecordIterator<InternalRow> readBatch() throws IOException {
        DataEvolutionRow row = new DataEvolutionRow(readers.length, rowOffsets, fieldOffsets);
        RecordIterator<InternalRow>[] iterators = new RecordIterator[readers.length];
        for (int i = 0; i < readers.length; i++) {
            RecordReader<InternalRow> reader = readers[i];
            if (reader != null) {
                RecordIterator<InternalRow> batch = reader.readBatch();
                if (batch == null) {
                    // all readers are aligned, as long as one returns null, the others will also
                    // have no data
                    return null;
                }
                iterators[i] = batch;
            }
        }
        return new DataEvolutionIterator(row, iterators);
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(readers);
        } catch (Exception e) {
            throw new IOException("Failed to close inner readers", e);
        }
    }
}

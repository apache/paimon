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
import org.apache.paimon.reader.RecordReader.RecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The batch which is made up by several batches, it assumes that all iterators are aligned, and as
 * long as one returns null, the others will also have no data.
 */
public class DataEvolutionIterator implements RecordIterator<InternalRow> {

    private final DataEvolutionRow row;
    private final RecordIterator<InternalRow>[] iterators;

    public DataEvolutionIterator(DataEvolutionRow row, RecordIterator<InternalRow>[] iterators) {
        this.row = row;
        this.iterators = iterators;
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i] != null) {
                InternalRow next = iterators[i].next();
                if (next == null) {
                    return null;
                }
                row.setRow(i, next);
            }
        }
        return row;
    }

    @Override
    public void releaseBatch() {
        for (RecordIterator<InternalRow> iterator : iterators) {
            if (iterator != null) {
                iterator.releaseBatch();
            }
        }
    }
}

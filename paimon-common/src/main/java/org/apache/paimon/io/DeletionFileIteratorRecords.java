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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.deletionvectors.DeletionFileRecordIterator;
import org.apache.paimon.reader.RecordIteratorToIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;

import java.util.Iterator;

/** {@link BundleRecords} for {@link DeletionFileRecordIterator}. */
public class DeletionFileIteratorRecords implements BundleRecords {

    private final DeletionFileRecordIterator iterator;

    public DeletionFileIteratorRecords(DeletionFileRecordIterator iterator) {
        this.iterator = iterator;
    }

    public DeletionFileRecordIterator deletionFileIterator() {
        return iterator;
    }

    @Override
    public long rowCount() {
        VectorizedColumnBatch batch = ((VectorizedRecordIterator) iterator).batch();
        return batch.getNumRows() - iterator.deletionVector().getCardinality();
    }

    @Override
    public Iterator<InternalRow> iterator() {
        return new RecordIteratorToIterator<>(iterator);
    }
}

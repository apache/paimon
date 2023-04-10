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

package org.apache.paimon.mergetree.region;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Record reader region manages {@link RecordReaderSubRegion} list, and each subregion is
 * constructed according to {@link org.apache.paimon.mergetree.SortedRun}. {@link SortedRegionDataRecordReader}s with
 * intersected key ranges in subregions must go to the same region, for example, there are two
 * subregions and each one has two {@link SortedRegionDataRecordReader} as follows
 *
 * <ul>
 *   <li>subregion1: reader11 with key range [1,100], reader12 with key range [200, 300]
 *   <li>subregion2: reader21 with key range [50, 250], reader 22 with key range [280, 400]
 * </ul>
 *
 * <p>reader11 and reader21, reader21 and reader12, reader12 and reader22 have intersected key
 * ranges.
 */
public class RecordReaderRegion<T> {
    private final List<RecordReaderSubRegion<T>> readers;
    private final Comparator<InternalRow> keyComparator;
    private InternalRow minKey;
    private InternalRow maxKey;

    public RecordReaderRegion(Comparator<InternalRow> keyComparator, int count) {
        this.keyComparator = keyComparator;
        this.readers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            this.readers.add(new RecordReaderSubRegion<>(i, new ArrayList<>()));
        }
    }

    public InternalRow getMinKey() {
        return minKey;
    }

    public InternalRow getMaxKey() {
        return maxKey;
    }

    public boolean mergeReader(SortedRegionDataRecordReader<T> reader, int index) {
        checkState(
                index < readers.size(),
                "The index[%s] of merged reader must be < the readers count[%s]",
                index,
                readers.size());
        if (minKey == null && maxKey == null) {
            minKey = reader.getMinKey();
            maxKey = reader.getMaxKey();
            readers.get(index).add(reader);
            return true;
        }

        checkState(minKey != null, "minKey is null");
        checkState(maxKey != null, "maxKey is null");
        // Each SortedDataFileRecordReader in SortedRun will be sorted by key, so the new reader
        // minKey must >= the one in the region
        checkState(
                keyComparator.compare(minKey, reader.getMinKey()) <= 0,
                "The added minKey in reader must be equal or larger than the minKey in region");
        if (keyComparator.compare(maxKey, reader.getMinKey()) >= 0) {
            readers.get(index).add(reader);
            if (keyComparator.compare(maxKey, reader.getMaxKey()) < 0) {
                maxKey = reader.getMaxKey();
            }
            return true;
        }

        return false;
    }

    public boolean isEmpty() {
        if (readers.isEmpty()) {
            return true;
        }

        for (RecordReaderSubRegion<T> reader : readers) {
            if (!reader.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    RecordReaderSubRegion<T> getReader(int index) {
        return readers.get(index);
    }

    public List<RecordReader<T>> getReaders() throws IOException {
        List<RecordReader<T>> readerList = new ArrayList<>();
        for (RecordReaderSubRegion<T> recordReaderSubRegion : readers) {
            if (!recordReaderSubRegion.isEmpty()) {
                readerList.add(recordReaderSubRegion.getReader());
            }
        }
        return readerList;
    }
}

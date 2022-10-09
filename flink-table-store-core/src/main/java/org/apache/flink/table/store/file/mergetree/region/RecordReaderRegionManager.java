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

package org.apache.flink.table.store.file.mergetree.region;

import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Record region manager will divide {@link RecordReaderSubRegion} into multiple {@link
 * RecordReaderRegion}s, {@link SortedRegionDataRecordReader}s in {@link RecordReaderSubRegion} with
 * intersected key ranges in subregions must go to the same region.
 */
public class RecordReaderRegionManager<T> {
    private final List<RecordReaderSubRegion<T>> sortedReaders;
    private final Comparator<RowData> keyComparator;

    public RecordReaderRegionManager(
            List<RecordReaderSubRegion<T>> sortedReaders, Comparator<RowData> keyComparator) {
        this.sortedReaders = sortedReaders;
        this.keyComparator = keyComparator;
    }

    public List<RecordReaderRegion<T>> getRegionList() {
        List<RecordReaderRegion<T>> regionList = new ArrayList<>();
        RecordReaderRegion<T> current =
                new RecordReaderRegion<>(keyComparator, sortedReaders.size());
        while (!isEmpty()) {
            // find the minimum minKey
            SortedRegionDataRecordReader<T> minKeyReader = null;
            int index = -1;
            for (RecordReaderSubRegion<T> reader : sortedReaders) {
                if (!reader.isEmpty()) {
                    SortedRegionDataRecordReader<T> minIndexReader = reader.peek();
                    if (minKeyReader == null
                            || keyComparator.compare(
                                            minKeyReader.getMinKey(), minIndexReader.getMinKey())
                                    > 0) {
                        minKeyReader = minIndexReader;
                        index = reader.getIndex();
                    }
                }
            }
            if (minKeyReader == null) {
                break;
            }
            sortedReaders.get(index).remove();
            if (!current.mergeReader(minKeyReader, index)) {
                checkState(!current.isEmpty(), "Current region can't be empty");
                regionList.add(current);
                current = new RecordReaderRegion<>(keyComparator, sortedReaders.size());
                checkState(current.mergeReader(minKeyReader, index));
            }
        }
        if (!current.isEmpty()) {
            regionList.add(current);
        }

        return regionList;
    }

    private boolean isEmpty() {
        for (RecordReaderSubRegion<T> indexReader : sortedReaders) {
            if (!indexReader.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}

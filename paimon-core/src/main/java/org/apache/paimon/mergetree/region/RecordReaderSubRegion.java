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
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link RecordReaderSubRegion} will first be created from {@link org.apache.paimon.mergetree.SortedRun} in which the
 * sortedReaders are according to the data files in {@link org.apache.paimon.mergetree.SortedRun}. Then the {@link
 * RecordReaderRegionManager} will divide {@link RecordReaderSubRegion} list into multiple {@link
 * RecordReaderRegion}. Each {@link RecordReaderRegion} includes {@link RecordReaderSubRegion} list.
 *
 * @param <T> the record type of the reader
 */
public class RecordReaderSubRegion<T> {
    private final int index;
    // The readers is created from data files in {@code SortedRun}, so it has been sorted by key.
    private final List<SortedRegionDataRecordReader<T>> sortedReaders;

    public RecordReaderSubRegion(int index, List<SortedRegionDataRecordReader<T>> sortedReaders) {
        this.index = index;
        this.sortedReaders = sortedReaders;
    }

    public boolean isEmpty() {
        return sortedReaders.isEmpty();
    }

    public SortedRegionDataRecordReader<T> peek() {
        return sortedReaders.get(0);
    }

    public int getIndex() {
        return index;
    }

    public void remove() {
        sortedReaders.remove(0);
    }

    public void add(SortedRegionDataRecordReader<T> reader) {
        sortedReaders.add(reader);
    }

    public RecordReader<T> getReader() throws IOException {
        if (sortedReaders.size() == 1) {
            return sortedReaders.get(0).getReader().get();
        } else {
            return ConcatRecordReader.create(
                    sortedReaders.stream()
                            .map(SortedRegionDataRecordReader::getReader)
                            .collect(Collectors.toList()));
        }
    }

    @VisibleForTesting
    List<SortedRegionDataRecordReader<T>> getSortedReaders() {
        return sortedReaders;
    }
}

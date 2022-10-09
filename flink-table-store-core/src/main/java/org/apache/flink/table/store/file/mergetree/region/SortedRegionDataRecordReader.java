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
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;

/**
 * Sorted data file record reader with key range.
 *
 * @param <T> the record type
 */
public class SortedRegionDataRecordReader<T> {
    private final ConcatRecordReader.ReaderSupplier<T> reader;

    private final RowData minKey;
    private final RowData maxKey;

    public SortedRegionDataRecordReader(
            ConcatRecordReader.ReaderSupplier<T> reader, RowData minKey, RowData maxKey) {
        this.reader = reader;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public ConcatRecordReader.ReaderSupplier<T> getReader() {
        return reader;
    }

    public RowData getMinKey() {
        return minKey;
    }

    public RowData getMaxKey() {
        return maxKey;
    }
}

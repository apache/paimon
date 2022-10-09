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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueFileReaderFactory;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionWrapper;
import org.apache.flink.table.store.file.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.flink.table.store.file.mergetree.compact.SortMergeReader;
import org.apache.flink.table.store.file.mergetree.region.RecordReaderRegion;
import org.apache.flink.table.store.file.mergetree.region.RecordReaderRegionManager;
import org.apache.flink.table.store.file.mergetree.region.RecordReaderSubRegion;
import org.apache.flink.table.store.file.mergetree.region.SortedRegionDataRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Utility class to create commonly used {@link RecordReader}s for merge trees. */
public class MergeTreeReaders {

    private MergeTreeReaders() {}

    public static RecordReader<KeyValue> readerForMergeTree(
            List<List<SortedRun>> sections,
            boolean dropDelete,
            KeyValueFileReaderFactory readerFactory,
            Comparator<RowData> userKeyComparator,
            MergeFunction<KeyValue> mergeFunction)
            throws IOException {
        List<ConcatRecordReader.ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            readers.add(
                    () ->
                            readerForSection(
                                    section,
                                    readerFactory,
                                    userKeyComparator,
                                    new ReducerMergeFunctionWrapper(mergeFunction)));
        }
        RecordReader<KeyValue> reader = ConcatRecordReader.create(readers);
        if (dropDelete) {
            reader = new DropDeleteReader(reader);
        }
        return reader;
    }

    public static RecordReader<KeyValue> readerForSection(
            List<SortedRun> section,
            KeyValueFileReaderFactory readerFactory,
            Comparator<RowData> userKeyComparator,
            MergeFunctionWrapper<KeyValue> mergeFunctionWrapper)
            throws IOException {
        List<RecordReaderSubRegion<KeyValue>> readerSubRegions = new ArrayList<>();
        int index = 0;
        for (SortedRun run : section) {
            readerSubRegions.add(readerSubRegionForRun(index++, run, readerFactory));
        }
        RecordReaderRegionManager<KeyValue> regionManager =
                new RecordReaderRegionManager<>(readerSubRegions, userKeyComparator);
        List<RecordReaderRegion<KeyValue>> regions = regionManager.getRegionList();
        List<ConcatRecordReader.ReaderSupplier<KeyValue>> supplierList = new ArrayList<>();
        for (RecordReaderRegion<KeyValue> region : regions) {
            List<RecordReader<KeyValue>> readerList = region.getReaders();
            supplierList.add(
                    () ->
                            readerList.size() == 1 ? readerList.get(0) : new SortMergeReader.create(
                                    readerList, userKeyComparator, mergeFunctionWrapper));
        }
        return ConcatRecordReader.create(supplierList);
    }

    public static RecordReaderSubRegion<KeyValue> readerSubRegionForRun(
            int index, SortedRun run, KeyValueFileReaderFactory readerFactory) {
        List<SortedRegionDataRecordReader<KeyValue>> readers = new ArrayList<>();
        for (DataFileMeta file : run.files()) {
            readers.add(
                    new SortedRegionDataRecordReader<>(
                            () -> readerFactory.createRecordReader(file.fileName(), file.level()),
                            file.minKey(),
                            file.maxKey()));
        }
        return new RecordReaderSubRegion<>(index, readers);
    }
}

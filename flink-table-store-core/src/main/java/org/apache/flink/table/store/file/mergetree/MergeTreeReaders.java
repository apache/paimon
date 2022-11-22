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
        List<RecordReader<KeyValue>> readers = new ArrayList<>();
        for (SortedRun run : section) {
            readers.add(readerForRun(run, readerFactory));
        }
        if (readers.size() == 1) {
            return readers.get(0);
        } else {
            return new SortMergeReader<>(readers, userKeyComparator, mergeFunctionWrapper);
        }
    }

    public static RecordReader<KeyValue> readerForRun(
            SortedRun run, KeyValueFileReaderFactory readerFactory) throws IOException {
        List<ConcatRecordReader.ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (DataFileMeta file : run.files()) {
            readers.add(
                    () ->
                            readerFactory.createRecordReader(
                                    file.schemaId(), file.fileName(), file.level()));
        }
        return ConcatRecordReader.create(readers);
    }
}

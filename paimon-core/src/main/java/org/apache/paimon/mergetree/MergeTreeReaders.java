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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Utility class to create commonly used {@link RecordReader}s for merge trees. */
public class MergeTreeReaders {

    private MergeTreeReaders() {}

    public static <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<ReaderSupplier<T>> readers = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            readers.add(
                    () ->
                            readerForSection(
                                    section,
                                    readerFactory,
                                    userKeyComparator,
                                    userDefinedSeqComparator,
                                    mergeFunctionWrapper,
                                    mergeSorter));
        }
        return ConcatRecordReader.create(readers);
    }

    public static <T> RecordReader<T> readerForSection(
            List<SortedRun> section,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (SortedRun run : section) {
            readers.add(() -> readerForRun(run, readerFactory));
        }
        return mergeSorter.mergeSort(
                readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
    }

    private static RecordReader<KeyValue> readerForRun(
            SortedRun run, FileReaderFactory<KeyValue> readerFactory) throws IOException {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (DataFileMeta file : run.files()) {
            readers.add(() -> readerFactory.createRecordReader(file));
        }
        return ConcatRecordReader.create(readers);
    }
}

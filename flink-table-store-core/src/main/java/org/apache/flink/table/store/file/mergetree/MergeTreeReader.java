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
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.SortMergeReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A {@link RecordReader} to read merge tree sections. */
public class MergeTreeReader implements RecordReader<KeyValue> {

    private final RecordReader<KeyValue> reader;

    private final boolean dropDelete;

    public MergeTreeReader(
            List<List<SortedRun>> sections,
            boolean dropDelete,
            DataFileReader dataFileReader,
            Comparator<RowData> userKeyComparator,
            MergeFunction mergeFunction)
            throws IOException {
        this.dropDelete = dropDelete;

        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            readers.add(
                    () ->
                            readerForSection(
                                    section, dataFileReader, userKeyComparator, mergeFunction));
        }
        this.reader = ConcatRecordReader.create(readers);
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = reader.readBatch();

        if (!dropDelete) {
            return batch;
        }

        if (batch == null) {
            return null;
        }

        return new RecordIterator<KeyValue>() {
            @Override
            public KeyValue next() throws IOException {
                while (true) {
                    KeyValue kv = batch.next();
                    if (kv == null) {
                        return null;
                    }

                    if (kv.valueKind() == RowKind.INSERT
                            || kv.valueKind() == RowKind.UPDATE_AFTER) {
                        return kv;
                    }
                }
            }

            @Override
            public void releaseBatch() {
                batch.releaseBatch();
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static RecordReader<KeyValue> readerForSection(
            List<SortedRun> section,
            DataFileReader dataFileReader,
            Comparator<RowData> userKeyComparator,
            MergeFunction mergeFunction)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>();
        for (SortedRun run : section) {
            readers.add(readerForRun(run, dataFileReader));
        }
        return SortMergeReader.create(readers, userKeyComparator, mergeFunction);
    }

    public static RecordReader<KeyValue> readerForRun(SortedRun run, DataFileReader dataFileReader)
            throws IOException {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (DataFileMeta file : run.files()) {
            readers.add(() -> dataFileReader.read(file.fileName()));
        }
        return ConcatRecordReader.create(readers);
    }
}

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
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.flink.table.store.file.mergetree.compact.SortMergeReader;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileReader;
import org.apache.flink.table.store.file.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A {@link RecordReader} to read merge tree sections. */
public class MergeTreeReader implements RecordReader {

    private final RecordReader reader;

    private final boolean dropDelete;

    public MergeTreeReader(
            List<List<SortedRun>> sections,
            boolean dropDelete,
            SstFileReader sstFileReader,
            Comparator<RowData> userKeyComparator,
            Accumulator accumulator)
            throws IOException {
        this.dropDelete = dropDelete;

        List<ReaderSupplier> readers = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            readers.add(
                    () -> readerForSection(section, sstFileReader, userKeyComparator, accumulator));
        }
        this.reader = ConcatRecordReader.create(readers);
    }

    @Nullable
    @Override
    public RecordIterator readBatch() throws IOException {
        RecordIterator batch = reader.readBatch();

        if (!dropDelete) {
            return batch;
        }

        if (batch == null) {
            return null;
        }

        return new RecordIterator() {
            @Override
            public KeyValue next() throws IOException {
                while (true) {
                    KeyValue kv = batch.next();
                    if (kv == null) {
                        return null;
                    }

                    if (kv.valueKind() == ValueKind.ADD) {
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

    public static RecordReader readerForSection(
            List<SortedRun> section,
            SstFileReader sstFileReader,
            Comparator<RowData> userKeyComparator,
            Accumulator accumulator)
            throws IOException {
        List<RecordReader> readers = new ArrayList<>();
        for (SortedRun run : section) {
            readers.add(readerForRun(run, sstFileReader));
        }
        return SortMergeReader.create(readers, userKeyComparator, accumulator);
    }

    public static RecordReader readerForRun(SortedRun run, SstFileReader sstFileReader)
            throws IOException {
        List<ReaderSupplier> readers = new ArrayList<>();
        for (SstFileMeta file : run.files()) {
            readers.add(() -> sstFileReader.read(file.fileName()));
        }
        return ConcatRecordReader.create(readers);
    }
}

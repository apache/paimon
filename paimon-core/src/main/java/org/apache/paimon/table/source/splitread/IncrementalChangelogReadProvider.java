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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.ReverseReader;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.LazyField;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.paimon.table.source.KeyValueTableRead.unwrap;

/** A {@link SplitReadProvider} to incremental changelog read. */
public class IncrementalChangelogReadProvider implements SplitReadProvider {

    private final LazyField<SplitRead<InternalRow>> splitRead;

    public IncrementalChangelogReadProvider(
            Supplier<MergeFileSplitRead> supplier,
            Consumer<SplitRead<InternalRow>> valuesAssigner) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            SplitRead<InternalRow> read = create(supplier);
                            valuesAssigner.accept(read);
                            return read;
                        });
    }

    private SplitRead<InternalRow> create(Supplier<MergeFileSplitRead> supplier) {
        final MergeFileSplitRead read = supplier.get().withReadKeyType(RowType.of());
        IOFunction<DataSplit, RecordReader<InternalRow>> convertedFactory =
                split -> {
                    RecordReader<KeyValue> reader =
                            ConcatRecordReader.create(
                                    () ->
                                            new ReverseReader(
                                                    read.createNoMergeReader(
                                                            split.partition(),
                                                            split.bucket(),
                                                            split.beforeFiles(),
                                                            split.beforeDeletionFiles()
                                                                    .orElse(null),
                                                            true)),
                                    () ->
                                            read.createNoMergeReader(
                                                    split.partition(),
                                                    split.bucket(),
                                                    split.dataFiles(),
                                                    split.deletionFiles().orElse(null),
                                                    true));
                    return unwrap(reader);
                };

        return SplitRead.convert(read, convertedFactory);
    }

    @Override
    public boolean match(DataSplit split, boolean forceKeepDelete) {
        return !split.beforeFiles().isEmpty() && split.isStreaming();
    }

    @Override
    public boolean initialized() {
        return splitRead.initialized();
    }

    @Override
    public SplitRead<InternalRow> getOrCreate() {
        return splitRead.get();
    }
}

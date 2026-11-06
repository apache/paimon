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
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LazyField;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.paimon.table.source.KeyValueTableRead.unwrap;

/** A {@link SplitReadProvider} to incremental changelog read. */
public class IncrementalChangelogReadProvider implements SplitReadProvider {

    private final LazyField<SplitRead<InternalRow>> splitRead;

    public IncrementalChangelogReadProvider(
            Supplier<MergeFileSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            SplitRead<InternalRow> read = create(supplier, splitReadConfig);
                            splitReadConfig.config(read);
                            return read;
                        });
    }

    private SplitRead<InternalRow> create(
            Supplier<MergeFileSplitRead> supplier, SplitReadConfig config) {
        final MergeFileSplitRead read = supplier.get().withReadKeyType(RowType.of());
        // A guard merge needs the complete primary key. Keep the existing key-free raw path
        // for ordinary splits, and configure the guarded reader only when one is encountered.
        LazyField<SplitRead<InternalRow>> guarded =
                new LazyField<>(
                        () -> {
                            MergeFileSplitRead merge = supplier.get();
                            SplitRead<InternalRow> converted =
                                    SplitRead.convert(
                                            merge,
                                            split -> createReader(merge, (IncrementalSplit) split));
                            config.config(converted);
                            return converted;
                        });
        return SplitRead.convert(
                read,
                split -> {
                    IncrementalSplit incremental = (IncrementalSplit) split;
                    if (incremental.beforeDeleteOrderingGuards().isEmpty()
                            && incremental.afterDeleteOrderingGuards().isEmpty()) {
                        return createReader(read, incremental);
                    }
                    SplitRead<InternalRow> guardRead = guarded.get();
                    config.config(guardRead);
                    return guardRead.createReader(split);
                });
    }

    private RecordReader<InternalRow> createReader(MergeFileSplitRead read, IncrementalSplit split)
            throws IOException {
        RecordReader<KeyValue> reader =
                ConcatRecordReader.create(
                        () ->
                                new ReverseReader(
                                        read.createMergeReader(
                                                split.partition(),
                                                split.bucket(),
                                                split.beforeFiles(),
                                                split.beforeDeletionFiles(),
                                                false,
                                                split.beforeDeleteOrderingGuards())),
                        () ->
                                read.createMergeReader(
                                        split.partition(),
                                        split.bucket(),
                                        split.afterFiles(),
                                        split.afterDeletionFiles(),
                                        false,
                                        split.afterDeleteOrderingGuards()));
        return unwrap(reader, read.tableSchema().options());
    }

    @Override
    public boolean match(Split split, Context context) {
        if (!(split instanceof IncrementalSplit)) {
            return false;
        }
        return ((IncrementalSplit) split).isStreaming();
    }

    @Override
    public LazyField<SplitRead<InternalRow>> get() {
        return splitRead;
    }
}

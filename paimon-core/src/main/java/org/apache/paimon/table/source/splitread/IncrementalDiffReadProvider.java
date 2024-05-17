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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.LazyField;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** A {@link SplitReadProvider} to batch incremental diff read. */
public class IncrementalDiffReadProvider implements SplitReadProvider {

    private final LazyField<SplitRead<InternalRow>> splitRead;

    public IncrementalDiffReadProvider(
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
        return new IncrementalDiffSplitRead(supplier.get());
    }

    @Override
    public boolean match(DataSplit split, boolean forceKeepDelete) {
        return !split.beforeFiles().isEmpty() && !split.isStreaming();
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

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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.FieldMergeSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.LazyField;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** A {@link SplitReadProvider} to create {@link MergeFieldSplitReadProvider}. */
public class MergeFieldSplitReadProvider implements SplitReadProvider {

    private final LazyField<FieldMergeSplitRead> splitRead;

    public MergeFieldSplitReadProvider(
            Supplier<FieldMergeSplitRead> supplier,
            Consumer<SplitRead<InternalRow>> valuesAssigner) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            FieldMergeSplitRead read = supplier.get();
                            valuesAssigner.accept(read);
                            return read;
                        });
    }

    @Override
    public boolean match(DataSplit split, boolean forceKeepDelete) {
        List<DataFileMeta> files = split.dataFiles();
        boolean onlyAppendFiles =
                files.stream()
                        .allMatch(
                                f ->
                                        f.fileSource().isPresent()
                                                && f.fileSource().get() == FileSource.APPEND
                                                && f.firstRowId() != null);
        if (onlyAppendFiles) {
            // contains same first row id, need merge fields
            return files.stream().mapToLong(DataFileMeta::firstRowId).distinct().count()
                    != files.size();
        }
        return false;
    }

    @Override
    public boolean initialized() {
        return splitRead.initialized();
    }

    @Override
    public RawFileSplitRead getOrCreate() {
        return splitRead.get();
    }
}

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
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.LazyField;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** A {@link SplitReadProvider} to create {@link RawFileSplitRead}. */
public class RawFileSplitReadProvider implements SplitReadProvider {

    private final LazyField<RawFileSplitRead> splitRead;

    public RawFileSplitReadProvider(
            Supplier<RawFileSplitRead> supplier, Consumer<SplitRead<InternalRow>> valuesAssigner) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            RawFileSplitRead read = supplier.get();
                            valuesAssigner.accept(read);
                            return read;
                        });
    }

    @Override
    public boolean match(DataSplit split, boolean forceKeepDelete) {
        boolean matched = !forceKeepDelete && !split.isStreaming() && split.rawConvertible();
        if (matched) {
            // for legacy version, we are not sure if there are delete rows, but in order to be
            // compatible with the query acceleration of the OLAP engine, we have generated raw
            // files.
            // Here, for the sake of correctness, we still need to perform drop delete filtering.
            for (DataFileMeta file : split.dataFiles()) {
                if (!file.deleteRowCount().isPresent()) {
                    return false;
                }
            }
        }
        return matched;
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

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

package org.apache.paimon.operation;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Read operation which provides {@link RecordReader} creation.
 *
 * @param <T> type of record to read.
 */
public interface SplitRead<T> {

    SplitRead<T> forceKeepDelete();

    SplitRead<T> withIOManager(@Nullable IOManager ioManager);

    SplitRead<T> withReadType(RowType readType);

    SplitRead<T> withFilter(@Nullable Predicate predicate);

    /** Create a {@link RecordReader} from split. */
    RecordReader<T> createReader(DataSplit split) throws IOException;

    static <L, R> SplitRead<R> convert(
            SplitRead<L> read, IOFunction<DataSplit, RecordReader<R>> convertedFactory) {
        return new SplitRead<R>() {
            @Override
            public SplitRead<R> forceKeepDelete() {
                read.forceKeepDelete();
                return this;
            }

            @Override
            public SplitRead<R> withIOManager(@Nullable IOManager ioManager) {
                read.withIOManager(ioManager);
                return this;
            }

            @Override
            public SplitRead<R> withReadType(RowType readType) {
                read.withReadType(readType);
                return this;
            }

            @Override
            public SplitRead<R> withFilter(@Nullable Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public RecordReader<R> createReader(DataSplit split) throws IOException {
                return convertedFactory.apply(split);
            }
        };
    }
}

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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MergeSorterOwnershipTest {
    @ParameterizedTest
    @ValueSource(strings = {"checked", "unchecked", "error"})
    void failedReaderConstructionClosesEveryPreviouslyOpenedReader(String kind) {
        IOException checked = new IOException("read failed");
        Throwable failure =
                kind.equals("checked")
                        ? checked
                        : kind.equals("unchecked")
                                ? new UncheckedIOException(checked)
                                : new AssertionError("reader construction failed");
        AtomicInteger closed = new AtomicInteger();
        RecordReader<KeyValue> first =
                new RecordReader<KeyValue>() {
                    @Override
                    public RecordIterator<KeyValue> readBatch() {
                        return null;
                    }

                    @Override
                    public void close() {
                        closed.incrementAndGet();
                    }
                };
        ReaderSupplier<KeyValue> failing =
                () -> {
                    if (failure instanceof IOException) {
                        throw (IOException) failure;
                    }
                    if (failure instanceof Error) {
                        throw (Error) failure;
                    }
                    throw (RuntimeException) failure;
                };
        List<ReaderSupplier<KeyValue>> readers = Arrays.asList(() -> first, failing);
        RowType row = RowType.builder().field("k", DataTypes.INT()).build();
        MergeSorter sorter = new MergeSorter(new CoreOptions(new Options()), row, row, null);
        assertThatThrownBy(
                        () ->
                                sorter.mergeSortNoSpill(
                                        readers,
                                        Comparator.comparingInt(key -> key.getInt(0)),
                                        null,
                                        null))
                .isSameAs(failure);
        assertThat(closed).hasValue(1);
    }
}

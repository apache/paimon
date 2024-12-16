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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * A {@link SingleFileWriter} which also produces statistics for each written field.
 *
 * @param <T> type of records to write.
 * @param <R> type of result to produce after writing a file.
 */
public abstract class StatsCollectingSingleFileWriter<T, R> extends SingleFileWriter<T, R> {

    @Nullable private final SimpleStatsExtractor simpleStatsExtractor;
    @Nullable private SimpleStatsCollector simpleStatsCollector = null;
    @Nullable private SimpleColStats[] noneStats = null;
    private final boolean isStatsDisabled;

    public StatsCollectingSingleFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<T, InternalRow> converter,
            RowType writeSchema,
            @Nullable SimpleStatsExtractor simpleStatsExtractor,
            String compression,
            SimpleColStatsCollector.Factory[] statsCollectors,
            boolean asyncWrite) {
        super(fileIO, factory, path, converter, compression, asyncWrite);
        this.simpleStatsExtractor = simpleStatsExtractor;
        if (this.simpleStatsExtractor == null) {
            this.simpleStatsCollector = new SimpleStatsCollector(writeSchema, statsCollectors);
        }
        Preconditions.checkArgument(
                statsCollectors.length == writeSchema.getFieldCount(),
                "The stats collector is not aligned to write schema.");
        this.isStatsDisabled =
                Arrays.stream(SimpleColStatsCollector.create(statsCollectors))
                        .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
        if (isStatsDisabled) {
            this.noneStats =
                    IntStream.range(0, statsCollectors.length)
                            .mapToObj(i -> SimpleColStats.NONE)
                            .toArray(SimpleColStats[]::new);
        }
    }

    @Override
    public void write(T record) throws IOException {
        InternalRow rowData = writeImpl(record);
        if (simpleStatsCollector != null && !simpleStatsCollector.isDisabled()) {
            simpleStatsCollector.collect(rowData);
        }
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        Preconditions.checkState(
                simpleStatsExtractor != null,
                "Can't write bundle without simpleStatsExtractor, we may lose all the statistical information");

        super.writeBundle(bundle);
    }

    public SimpleColStats[] fieldStats() throws IOException {
        Preconditions.checkState(closed, "Cannot access metric unless the writer is closed.");
        if (simpleStatsExtractor != null) {
            if (isStatsDisabled) {
                return noneStats;
            } else {
                return simpleStatsExtractor.extract(fileIO, path);
            }
        } else {
            return simpleStatsCollector.extract();
        }
    }
}

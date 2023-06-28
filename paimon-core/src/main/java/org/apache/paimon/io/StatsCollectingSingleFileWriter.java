/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsCollector;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.NoneStats;
import org.apache.paimon.statistics.Stats;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * A {@link SingleFileWriter} which also produces statistics for each written field.
 *
 * @param <T> type of records to write.
 * @param <R> type of result to produce after writing a file.
 */
public abstract class StatsCollectingSingleFileWriter<T, R> extends SingleFileWriter<T, R> {

    @Nullable private final TableStatsExtractor tableStatsExtractor;
    @Nullable private TableStatsCollector tableStatsCollector = null;
    private final boolean isStatsCollectorDisabled;

    public StatsCollectingSingleFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<T, InternalRow> converter,
            RowType writeSchema,
            @Nullable TableStatsExtractor tableStatsExtractor,
            String compression,
            Stats[] stats) {
        super(fileIO, factory, path, converter, compression);
        this.tableStatsExtractor = tableStatsExtractor;
        this.isStatsCollectorDisabled = Arrays.stream(stats).allMatch(p -> p instanceof NoneStats);
        if (this.tableStatsExtractor == null) {
            this.tableStatsCollector = new TableStatsCollector(writeSchema, stats);
        }
        Preconditions.checkArgument(
                stats.length == writeSchema.getFieldCount(),
                "The stats is not aligned to write schema.");
    }

    @Override
    public void write(T record) throws IOException {
        InternalRow rowData = writeImpl(record);
        if (tableStatsCollector != null && !isStatsCollectorDisabled) {
            tableStatsCollector.collect(rowData);
        }
    }

    public FieldStats[] fieldStats() throws IOException {
        Preconditions.checkState(closed, "Cannot access metric unless the writer is closed.");
        if (tableStatsExtractor != null) {
            return tableStatsExtractor.extract(fileIO, path);
        } else {
            return tableStatsCollector.extract();
        }
    }
}

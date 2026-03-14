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
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link InternalRow}.
 * Also produces {@link DataFileMeta} after writing a file.
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId;
    private final LongCounter seqNumCounter;
    private final boolean isExternalPath;
    private final SimpleStatsConverter statsArraySerializer;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;
    private final FileSource fileSource;
    @Nullable private final List<String> writeCols;
    private final int seqNumberFieldIndex;
    private long minSeqNumber;
    private long maxSeqNumber;
    private boolean hasNullSeqNumber;

    public RowDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType writeSchema,
            long schemaId,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            boolean isExternalPath,
            @Nullable List<String> writeCols) {
        super(fileIO, context, path, Function.identity(), writeSchema, asyncFileWrite);
        this.schemaId = schemaId;
        this.seqNumCounter = seqNumCounterSupplier.get();
        this.isExternalPath = isExternalPath;
        this.statsArraySerializer = new SimpleStatsConverter(writeSchema, statsDenseStore);
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), writeSchema, fileIndexOptions);
        this.fileSource = fileSource;
        this.writeCols = writeCols;
        this.seqNumberFieldIndex = writeSchema.getFieldIndex(SpecialFields.SEQUENCE_NUMBER.name());
        this.minSeqNumber = Long.MAX_VALUE;
        this.maxSeqNumber = Long.MIN_VALUE;
        this.hasNullSeqNumber = false;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        // add row to index if needed
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(row);
        }
        updateSeqNumber(row);
    }

    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }

    @Override
    public DataFileMeta result() throws IOException {
        long fileSize = outputBytes();
        Pair<List<String>, SimpleStats> statsPair =
                statsArraySerializer.toBinary(fieldStats(fileSize));
        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();
        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.forAppend(
                path.getName(),
                fileSize,
                recordCount(),
                statsPair.getRight(),
                minSeqNumber(),
                maxSeqNumber(),
                schemaId,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                indexResult.embeddedIndexBytes(),
                fileSource,
                statsPair.getKey(),
                externalPath,
                null,
                writeCols);
    }

    private long minSeqNumber() {
        if (seqNumberFieldIndex == -1) {
            return seqNumCounter.getValue() - super.recordCount();
        }
        // minSeqNumber stays at Long.MAX_VALUE when all records have null sequence numbers.
        // Returning 0 triggers RowTrackingCommitUtils.assignSnapshotId() to use snapshot ID.
        return minSeqNumber == Long.MAX_VALUE ? 0 : minSeqNumber;
    }

    private long maxSeqNumber() {
        if (seqNumberFieldIndex == -1) {
            return seqNumCounter.getValue() - 1;
        }
        // When hasNullSeqNumber is true, some records have null sequence numbers.
        // Returning 0 triggers RowTrackingCommitUtils.assignSnapshotId() to use snapshot ID for
        // max.
        return hasNullSeqNumber ? 0 : maxSeqNumber;
    }

    private void updateSeqNumber(InternalRow row) {
        seqNumCounter.add(1L);

        // If sequence number field exists, extract min/max from row data
        if (seqNumberFieldIndex != -1 && !row.isNullAt(seqNumberFieldIndex)) {
            long seqNum = row.getLong(seqNumberFieldIndex);
            minSeqNumber = Math.min(minSeqNumber, seqNum);
            maxSeqNumber = Math.max(maxSeqNumber, seqNum);
        } else if (seqNumberFieldIndex != -1) {
            // Manifest will calculate the correct max based on snapshot id
            hasNullSeqNumber = true;
        }
    }
}

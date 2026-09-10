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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.function.LongPredicate;
import java.util.function.Supplier;

/** {@link RollingFileWriterImpl} for data files containing {@link InternalRow}. */
public class RowDataRollingFileWriter extends RollingFileWriterImpl<InternalRow, DataFileMeta> {

    @Nullable private LongPredicate fileRollingPredicate;
    private boolean pendingRoll;

    public RowDataRollingFileWriter(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            String fileCompression,
            SimpleColStatsCollector.Factory[] statsCollectors,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            @Nullable List<String> writeCols,
            @Nullable FileFormat rowSidecarFormat,
            long targetFileRowNum) {
        super(
                new Supplier<RowDataFileWriter>() {

                    private final FormatWriterFactory formatWriterFactory =
                            fileFormat.createWriterFactory(writeSchema);

                    @Override
                    public RowDataFileWriter get() {
                        Path dataPath = pathFactory.newPath();
                        Path rowSidecarPath =
                                rowSidecarFormat == null
                                        ? null
                                        : new Path(
                                                dataPath.getParent(), dataPath.getName() + ".row");
                        FileWriterContext writerContext =
                                new FileWriterContext(
                                        formatWriterFactory,
                                        RollingFileWriter.createStatsProducer(
                                                fileFormat, writeSchema, statsCollectors),
                                        fileCompression);
                        return new RowDataFileWriter(
                                fileIO,
                                writerContext,
                                dataPath,
                                writeSchema,
                                schemaId,
                                seqNumCounterSupplier,
                                fileIndexOptions,
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                pathFactory.isExternalPath(),
                                writeCols,
                                rowSidecarFormat,
                                rowSidecarPath);
                    }
                },
                targetFileSize,
                targetFileRowNum);
    }

    /**
     * Restricts automatic rolling to accepted boundaries, expressed as the cumulative number of
     * records written by this writer. Must be configured before writing; closing the writer still
     * closes the final file regardless of the predicate.
     */
    public RowDataRollingFileWriter withFileRollingPredicate(LongPredicate predicate) {
        Preconditions.checkState(recordCount() == 0, "Must configure rolling before writing.");
        this.fileRollingPredicate = Preconditions.checkNotNull(predicate);
        return this;
    }

    @Override
    protected void beforeWrite(InternalRow row) throws IOException {
        if (pendingRoll && fileRollingPredicate.test(recordCount())) {
            closeCurrentWriter();
        }
    }

    @Override
    protected void onRollingCondition(InternalRow row) throws IOException {
        if (fileRollingPredicate == null || fileRollingPredicate.test(recordCount())) {
            closeCurrentWriter();
        } else {
            pendingRoll = true;
        }
    }

    @Override
    protected void onCurrentWriterClosed() {
        pendingRoll = false;
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (fileRollingPredicate == null) {
            super.writeBundle(bundle);
        } else {
            for (InternalRow row : bundle) {
                write(row);
            }
        }
    }
}

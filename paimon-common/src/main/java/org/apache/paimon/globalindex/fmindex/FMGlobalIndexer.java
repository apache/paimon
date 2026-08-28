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

package org.apache.paimon.globalindex.fmindex;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.UnionGlobalIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeFamily;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Exact byte-oriented FM index with independently readable Lance-style partitions. */
public class FMGlobalIndexer implements GlobalIndexer {

    private final int partitionTextLength;
    private final int partitionRowCount;
    private final int sampleRate;
    private final boolean storeVerificationValues;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final FMIndexReadContext readContext;
    private final int demandPageSize;
    private final double locateCostRatio;

    public FMGlobalIndexer(DataField dataField, Options options) {
        checkArgument(
                dataField.type().is(DataTypeFamily.CHARACTER_STRING),
                "FM index requires a character string column, but field '%s' is %s.",
                dataField.name(),
                dataField.type());
        long configuredPartitionSize = options.get(FMGlobalIndexOptions.PARTITION_SIZE).getBytes();
        checkArgument(
                configuredPartitionSize >= 2 && configuredPartitionSize < Integer.MAX_VALUE,
                "FM index partition size must be in [2, %s).",
                Integer.MAX_VALUE);
        this.partitionTextLength = (int) configuredPartitionSize;
        this.partitionRowCount = options.get(FMGlobalIndexOptions.PARTITION_ROW_COUNT);
        checkArgument(partitionRowCount > 0, "FM index partition row count must be positive.");
        this.sampleRate = options.get(FMGlobalIndexOptions.SA_SAMPLE_RATE);
        checkArgument(
                sampleRate > 0 && sampleRate <= 1024 && (sampleRate & (sampleRate - 1)) == 0,
                "FM index SA sample rate must be a power of two in [1, 1024].");
        this.storeVerificationValues = options.get(FMGlobalIndexOptions.STORE_VERIFICATION_VALUES);
        CompressOptions compression =
                new CompressOptions(
                        options.get(FMGlobalIndexOptions.COMPRESSION),
                        options.get(FMGlobalIndexOptions.COMPRESSION_LEVEL));
        this.compressionFactory = BlockCompressionFactory.create(compression);
        long cacheSize = options.get(FMGlobalIndexOptions.READ_CACHE_SIZE).getBytes();
        checkArgument(cacheSize >= 0, "FM index read cache size must be non-negative.");
        this.readContext = new FMIndexReadContext(cacheSize);
        long configuredDemandPageSize =
                options.get(FMGlobalIndexOptions.DEMAND_PAGE_SIZE).getBytes();
        checkArgument(
                configuredDemandPageSize >= FMIndexFile.MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH
                        && configuredDemandPageSize <= 64L * 1024 * 1024,
                "FM index demand page size must be in [64 KiB, 64 MiB].");
        this.demandPageSize = (int) configuredDemandPageSize;
        this.locateCostRatio = options.get(FMGlobalIndexOptions.LOCATE_COST_RATIO);
        checkArgument(
                !Double.isNaN(locateCostRatio)
                        && !Double.isInfinite(locateCostRatio)
                        && locateCostRatio > 0d
                        && locateCostRatio <= 1d,
                "FM index locate cost ratio must be in (0, 1].");
    }

    @Override
    public FMGlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        return new FMGlobalIndexWriter(
                fileWriter,
                partitionTextLength,
                partitionRowCount,
                sampleRate,
                storeVerificationValues,
                compressionFactory);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            ExecutorService executor) {
        checkArgument(totalRowCount >= 0, "FM index total row count must be non-negative.");
        if (files.isEmpty()) {
            checkArgument(
                    totalRowCount == 0,
                    "FM index files are missing for %s source rows.",
                    totalRowCount);
            return FMGlobalIndexReader.empty(
                    executor, readContext, demandPageSize, locateCostRatio);
        }
        checkArgument(totalRowCount > 0, "FM index files cannot cover zero source rows.");
        FMGlobalIndexReader.FileSetRowCountValidator validator =
                new FMGlobalIndexReader.FileSetRowCountValidator(files.size(), totalRowCount);
        FMIndexFile.IndexMeta[] indexMetas = new FMIndexFile.IndexMeta[files.size()];
        for (int i = 0; i < files.size(); i++) {
            byte[] metadata = files.get(i).metadata();
            checkArgument(
                    metadata != null && metadata.length > 0,
                    "FM index container metadata is missing for %s.",
                    files.get(i).filePath());
            indexMetas[i] = FMIndexFile.readIndexMeta(metadata);
            validator.validate(
                    i, indexMetas[i].rowCount, indexMetas[i].firstRowId, indexMetas[i].lastRowId());
        }
        List<GlobalIndexReader> readers = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            FMGlobalIndexReader.ContainerMetadataLoader container =
                    new FMGlobalIndexReader.ContainerMetadataLoader(files.get(i), indexMetas[i]);
            for (FMIndexFile.PartitionMeta partition : indexMetas[i].partitions) {
                readers.add(
                        new FMGlobalIndexReader(
                                fileReader,
                                files.get(i),
                                executor,
                                readContext,
                                container,
                                partition,
                                demandPageSize,
                                locateCostRatio));
            }
        }
        return readers.size() == 1 ? readers.get(0) : new UnionGlobalIndexReader(readers);
    }
}

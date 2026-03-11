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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.KvCompactionManagerFactory;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.utils.FileStorePathFactory.createFormatPathFactories;

/** {@link FileStoreWrite} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreWrite.class);

    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final RowType valueType;
    private final String commitUser;
    private final KvCompactionManagerFactory compactManagerFactory;

    public KeyValueFileStoreWrite(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String commitUser,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<FieldsComparator> udsComparatorSupplier,
            Supplier<RecordEqualiser> logDedupEqualSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileStorePathFactory pathFactory,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable DynamicBucketIndexMaintainer.Factory dbMaintainerFactory,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            CoreOptions options,
            KeyValueFieldsExtractor extractor,
            String tableName) {
        super(
                snapshotManager,
                scan,
                options,
                partitionType,
                dbMaintainerFactory,
                dvMaintainerFactory,
                tableName);
        this.valueType = valueType;
        this.commitUser = commitUser;

        KeyValueFileReaderFactory.Builder readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schema,
                        keyType,
                        valueType,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        extractor,
                        options);
        RecordLevelExpire recordLevelExpire =
                RecordLevelExpire.create(options, schema, schemaManager);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schema.id(),
                        keyType,
                        valueType,
                        fileFormat(options),
                        createFormatPathFactories(options, formatPathFactory),
                        options.targetFileSize(true));
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
        this.compactManagerFactory =
                KvCompactionManagerFactory.create(
                        readerFactoryBuilder,
                        writerFactoryBuilder,
                        keyComparatorSupplier,
                        udsComparatorSupplier,
                        logDedupEqualSupplier,
                        mfFactory,
                        options,
                        keyType,
                        valueType,
                        partitionType,
                        fileIO,
                        schemaManager,
                        schema,
                        recordLevelExpire,
                        cacheManager);
    }

    @Override
    public KeyValueFileStoreWrite withIOManager(IOManager ioManager) {
        super.withIOManager(ioManager);
        compactManagerFactory.withIOManager(ioManager);
        if (mfFactory instanceof LookupMergeFunction.Factory) {
            ((LookupMergeFunction.Factory) mfFactory).withIOManager(ioManager);
        }
        return this;
    }

    @Override
    public FileStoreWrite<KeyValue> withMetricRegistry(MetricRegistry metricRegistry) {
        super.withMetricRegistry(metricRegistry);
        compactManagerFactory.withCompactionMetrics(compactionMetrics);
        return this;
    }

    @Override
    protected MergeTreeWriter createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating merge tree writer for partition {} bucket {} from restored files {}",
                    partition,
                    bucket,
                    restoreFiles);
        }

        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
        CompactManager compactManager =
                compactManagerFactory.create(
                        partition, bucket, compactExecutor, restoreFiles, dvMaintainer);

        return new MergeTreeWriter(
                options.writeBufferSpillable(),
                options.writeBufferSpillDiskSize(),
                options.localSortMaxNumFileHandles(),
                options.spillCompressOptions(),
                ioManager,
                compactManager,
                restoredMaxSeqNumber,
                keyComparator,
                mfFactory.create(),
                writerFactory,
                options.commitForceCompact(),
                options.changelogProducer(),
                restoreIncrement,
                UserDefinedSeqComparator.create(valueType, options));
    }

    @Override
    protected Function<WriterContainer<KeyValue>, Boolean> createWriterCleanChecker() {
        return createConflictAwareWriterCleanChecker(commitUser, restore);
    }

    @Override
    public void close() throws Exception {
        super.close();
        compactManagerFactory.close();
    }
}

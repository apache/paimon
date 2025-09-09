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

package org.apache.paimon.postpone;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.avro.AvroSchemaConverter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.MemoryFileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.utils.FileStorePathFactory.createFormatPathFactories;

/** {@link FileStoreWrite} for {@code bucket = -2} tables. */
public class PostponeBucketFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(PostponeBucketFileStoreWrite.class);

    private final CoreOptions options;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private boolean forceBufferSpill = false;

    public PostponeBucketFileStoreWrite(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            TableSchema schema,
            String commitUser,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            MergeFunctionFactory<KeyValue> mfFactory,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            String tableName,
            @Nullable Integer writeId) {
        super(snapshotManager, scan, options, partitionType, null, null, tableName);
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.mfFactory = mfFactory;
        this.readerFactoryBuilder = readerFactoryBuilder;

        Options newOptions = new Options(options.toMap());
        try {
            // use avro for postpone bucket
            AvroSchemaConverter.convertToSchema(schema.logicalRowType(), new HashMap<>());
            newOptions.set(CoreOptions.FILE_FORMAT, "avro");
            newOptions.set(CoreOptions.METADATA_STATS_MODE, "none");
        } catch (Exception e) {
            // ignored, avro does not support certain types in schema
        }
        // Each writer should have its unique prefix, so files from the same writer can be consumed
        // by the same compaction reader to keep the input order.
        // Also note that, for Paimon CDC, this object might be created multiple times in the same
        // job, however the object will always stay in the same thread, so we use hash of thread
        // name as the identifier.
        newOptions.set(
                CoreOptions.DATA_FILE_PREFIX,
                String.format(
                        "%s-u-%s-s-%d-w-",
                        options.dataFilePrefix(),
                        commitUser,
                        writeId == null
                                ? ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)
                                : writeId));
        this.options = new CoreOptions(newOptions);

        FileFormat fileFormat = fileFormat(this.options);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schema.id(),
                        keyType,
                        valueType,
                        fileFormat,
                        createFormatPathFactories(this.options, formatPathFactory),
                        this.options.targetFileSize(true));

        // Ignoring previous files saves scanning time.
        //
        // For postpone bucket tables, we only append new files to bucket = -2 directories.
        //
        // Also, we don't need to know current largest sequence id, because when compacting these
        // files, we will read the records file by file without merging, and then give them to
        // normal bucket writers.
        //
        // Because there is no merging when reading, sequence id across files are useless.
        withIgnorePreviousFiles(true);
    }

    @Override
    public PostponeBucketFileStoreWrite withIOManager(IOManager ioManager) {
        super.withIOManager(ioManager);
        if (mfFactory instanceof LookupMergeFunction.Factory) {
            ((LookupMergeFunction.Factory) mfFactory).withIOManager(ioManager);
        }
        return this;
    }

    @Override
    protected void forceBufferSpill() throws Exception {
        if (ioManager == null) {
            return;
        }
        if (forceBufferSpill) {
            return;
        }
        forceBufferSpill = true;
        LOG.info(
                "Force buffer spill for postpone file store write, writer number is: {}",
                writers.size());
        for (Map<Integer, WriterContainer<KeyValue>> bucketWriters : writers.values()) {
            for (WriterContainer<KeyValue> writerContainer : bucketWriters.values()) {
                ((PostponeBucketWriter) writerContainer.writer).toBufferedWriter();
            }
        }
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        // see comments in constructor
        super.withIgnorePreviousFiles(true);
    }

    @Override
    protected PostponeBucketWriter createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
        Preconditions.checkArgument(bucket == BucketMode.POSTPONE_BUCKET);
        Preconditions.checkArgument(
                restoreFiles.isEmpty(),
                "Postpone bucket writers should not restore previous files. This is unexpected.");
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        return new PostponeBucketWriter(
                fileIO,
                pathFactory.createDataFilePathFactory(partition, bucket),
                options.spillCompressOptions(),
                options.writeBufferSpillDiskSize(),
                ioManager,
                mfFactory.create(),
                writerFactory,
                files -> newFileRead(partition, bucket, files),
                forceBufferSpill,
                forceBufferSpill,
                restoreIncrement);
    }

    private RecordReaderIterator<KeyValue> newFileRead(
            BinaryRow partition, int bucket, List<DataFileMeta> files) throws IOException {
        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(partition, bucket, name -> Optional.empty());
        List<ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(() -> readerFactory.createRecordReader(file));
        }
        return new RecordReaderIterator<>(ConcatRecordReader.create(suppliers));
    }

    @Override
    protected Function<WriterContainer<KeyValue>, Boolean> createWriterCleanChecker() {
        return createNoConflictAwareWriterCleanChecker();
    }

    public static int getWriteId(String fileName) {
        try {
            String[] parts = fileName.split("-s-");
            return Integer.parseInt(parts[1].substring(0, parts[1].indexOf('-')));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Data file name "
                            + fileName
                            + " does not match the pattern. This is unexpected.",
                    e);
        }
    }
}

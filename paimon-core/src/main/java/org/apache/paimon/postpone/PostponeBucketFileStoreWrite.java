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
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.paimon.utils.FileStorePathFactory.createFormatPathFactories;

/** {@link FileStoreWrite} for {@code bucket = -2} tables. */
public class PostponeBucketFileStoreWrite extends AbstractFileStoreWrite<KeyValue> {

    private final CoreOptions options;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;

    public PostponeBucketFileStoreWrite(
            FileIO fileIO,
            TableSchema schema,
            String commitUser,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            String tableName) {
        super(snapshotManager, scan, null, null, tableName, options, partitionType);

        Options newOptions = new Options(options.toMap());
        // use avro for postpone bucket
        newOptions.set(CoreOptions.FILE_FORMAT, "avro");
        newOptions.set(CoreOptions.METADATA_STATS_MODE, "none");
        // each writer should have its unique prefix, so files from the same writer can be consumed
        // by the same compaction reader to keep the input order
        newOptions.set(
                CoreOptions.DATA_FILE_PREFIX,
                String.format(
                        "%s-u-%s-s-%d-w-",
                        options.dataFilePrefix(),
                        commitUser,
                        ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)));
        this.options = new CoreOptions(newOptions);

        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schema.id(),
                        keyType,
                        valueType,
                        this.options.fileFormat(),
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
    protected PostponeBucketWriter createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer) {
        Preconditions.checkArgument(bucket == BucketMode.POSTPONE_BUCKET);
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        return new PostponeBucketWriter(writerFactory);
    }

    @Override
    protected Function<WriterContainer<KeyValue>, Boolean> createWriterCleanChecker() {
        return createNoConflictAwareWriterCleanChecker();
    }
}

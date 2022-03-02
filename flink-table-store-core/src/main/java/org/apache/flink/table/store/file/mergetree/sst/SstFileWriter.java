/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RollingFile;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Writes {@link KeyValue}s into sst files. */
public class SstFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileWriter.class);

    private final RowType keyType;
    private final RowType valueType;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final SstPathFactory pathFactory;
    private final long suggestedFileSize;

    private SstFileWriter(
            RowType keyType,
            RowType valueType,
            BulkWriter.Factory<RowData> writerFactory,
            SstPathFactory pathFactory,
            long suggestedFileSize) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.writerFactory = writerFactory;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    @VisibleForTesting
    public SstPathFactory pathFactory() {
        return pathFactory;
    }

    /**
     * Write several {@link KeyValue}s into an sst file of a given level.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<SstFileMeta> write(CloseableIterator<KeyValue> iterator, int level)
            throws Exception {
        List<SstFileMeta> result = new ArrayList<>();
        List<Path> filesToCleanUp = new ArrayList<>();
        try {
            RollingFile.write(
                    iterator,
                    suggestedFileSize,
                    new StatsCollectingRollingFileContext(level),
                    result,
                    filesToCleanUp);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing sst files. Cleaning up.", e);
            for (Path path : filesToCleanUp) {
                FileUtils.deleteOrWarn(path);
            }
            throw e;
        } finally {
            iterator.close();
        }
        return result;
    }

    public void delete(SstFileMeta file) {
        FileUtils.deleteOrWarn(pathFactory.toPath(file.fileName()));
    }

    private abstract class AbstractRollingFileContext
            implements RollingFile.Context<KeyValue, SstFileMeta> {

        private final int level;
        private final KeyValueSerializer serializer;
        private final RowDataSerializer keySerializer;

        private long rowCount;
        private BinaryRowData minKey;
        private RowData maxKey;
        private long minSequenceNumber;
        private long maxSequenceNumber;

        private AbstractRollingFileContext(int level) {
            this.level = level;
            this.serializer = new KeyValueSerializer(keyType, valueType);
            this.keySerializer = new RowDataSerializer(keyType);
            resetMeta();
        }

        @Override
        public Path newPath() {
            return pathFactory.newPath();
        }

        @Override
        public BulkWriter<RowData> newWriter(FSDataOutputStream out) throws IOException {
            return writerFactory.create(out);
        }

        @Override
        public RowData serialize(KeyValue kv) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing key-value to sst file, kv: " + kv.toString(keyType, valueType));
            }

            rowCount++;
            if (minKey == null) {
                minKey = keySerializer.toBinaryRow(kv.key()).copy();
            }
            maxKey = kv.key();
            minSequenceNumber = Math.min(minSequenceNumber, kv.sequenceNumber());
            maxSequenceNumber = Math.max(maxSequenceNumber, kv.sequenceNumber());

            return serializer.toRow(kv);
        }

        @Override
        public SstFileMeta collectFile(Path path) throws IOException {
            SstFileMeta result =
                    new SstFileMeta(
                            path.getName(),
                            FileUtils.getFileSize(path),
                            rowCount,
                            minKey,
                            keySerializer.toBinaryRow(maxKey).copy(),
                            collectStats(path),
                            minSequenceNumber,
                            maxSequenceNumber,
                            level);
            resetMeta();
            return result;
        }

        private void resetMeta() {
            rowCount = 0;
            minKey = null;
            maxKey = null;
            minSequenceNumber = Long.MAX_VALUE;
            maxSequenceNumber = Long.MIN_VALUE;
        }

        protected abstract FieldStats[] collectStats(Path path);
    }

    private class StatsCollectingRollingFileContext extends AbstractRollingFileContext {

        private StatsCollectingRollingFileContext(int level) {
            super(level);
        }

        @Override
        protected FieldStats[] collectStats(Path path) {
            // TODO
            //  1. Read statistics directly from the written orc/parquet files.
            //  2. For other file formats use StatsCollector. Make sure fields are not reused
            //     otherwise we need copying.
            FieldStats[] stats = new FieldStats[valueType.getFieldCount()];
            for (int i = 0; i < stats.length; i++) {
                stats[i] = new FieldStats(null, null, 0);
            }
            return stats;
        }
    }

    /** Creates {@link SstFileWriter}. */
    public static class Factory {

        private final RowType keyType;
        private final RowType valueType;
        private final BulkWriter.Factory<RowData> writerFactory;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.keyType = keyType;
            this.valueType = valueType;
            RowType recordType = KeyValue.schema(keyType, valueType);
            this.writerFactory = fileFormat.createWriterFactory(recordType);
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public SstFileWriter create(BinaryRowData partition, int bucket) {
            return new SstFileWriter(
                    keyType,
                    valueType,
                    writerFactory,
                    pathFactory.createSstPathFactory(partition, bucket),
                    suggestedFileSize);
        }
    }
}

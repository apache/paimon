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
import org.apache.flink.core.fs.FileSystem;
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

        RollingFile rollingFile = null;
        Path currentPath = null;
        try {
            while (iterator.hasNext()) {
                if (rollingFile == null) {
                    currentPath = pathFactory.newPath();
                    rollingFile = new RollingFile(currentPath, suggestedFileSize);
                }
                rollingFile.write(iterator.next());
                if (rollingFile.exceedsSuggestedFileSize()) {
                    result.add(rollingFile.finish(level));
                    rollingFile = null;
                }
            }
            // finish last file
            if (rollingFile != null) {
                result.add(rollingFile.finish(level));
            }
            iterator.close();
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing sst files. Cleaning up.", e);
            // clean up finished files
            for (SstFileMeta meta : result) {
                FileUtils.deleteOrWarn(pathFactory.toPath(meta.fileName()));
            }
            // clean up in-progress file
            if (currentPath != null) {
                FileUtils.deleteOrWarn(currentPath);
            }
            throw e;
        }

        return result;
    }

    public void delete(SstFileMeta file) {
        FileUtils.deleteOrWarn(pathFactory.toPath(file.fileName()));
    }

    private class RollingFile {
        private final Path path;
        private final long suggestedFileSize;

        private final FSDataOutputStream out;
        private final BulkWriter<RowData> writer;
        private final KeyValueSerializer serializer;
        private final RowDataSerializer keySerializer;

        private long rowCount;
        private BinaryRowData minKey;
        private RowData maxKey;
        private long minSequenceNumber;
        private long maxSequenceNumber;

        private RollingFile(Path path, long suggestedFileSize) throws IOException {
            this.path = path;
            this.suggestedFileSize = suggestedFileSize;

            this.out =
                    this.path.getFileSystem().create(this.path, FileSystem.WriteMode.NO_OVERWRITE);
            this.writer = writerFactory.create(out);
            this.serializer = new KeyValueSerializer(keyType, valueType);
            this.keySerializer = new RowDataSerializer(keyType);

            this.rowCount = 0;
            this.minKey = null;
            this.maxKey = null;
            this.minSequenceNumber = Long.MAX_VALUE;
            this.maxSequenceNumber = Long.MIN_VALUE;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Creating new sst file " + path);
            }
        }

        private void write(KeyValue kv) throws IOException {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Writing key-value to sst file "
                                + path
                                + ", kv: "
                                + kv.toString(keyType, valueType));
            }

            writer.addElement(serializer.toRow(kv));
            rowCount++;
            if (minKey == null) {
                minKey = keySerializer.toBinaryRow(kv.key()).copy();
            }
            maxKey = kv.key();
            minSequenceNumber = Math.min(minSequenceNumber, kv.sequenceNumber());
            maxSequenceNumber = Math.max(maxSequenceNumber, kv.sequenceNumber());
        }

        private boolean exceedsSuggestedFileSize() throws IOException {
            // NOTE: this method is inaccurate for formats buffering changes in memory
            return out.getPos() >= suggestedFileSize;
        }

        private SstFileMeta finish(int level) throws IOException {
            writer.finish();
            out.close();

            // TODO
            //  1. Read statistics directly from the written orc/parquet files.
            //  2. For other file formats use StatsCollector. Make sure fields are not reused
            //     otherwise we need copying.
            FieldStats[] stats = new FieldStats[valueType.getFieldCount()];
            for (int i = 0; i < stats.length; i++) {
                stats[i] = new FieldStats(null, null, 0);
            }

            return new SstFileMeta(
                    path.getName(),
                    FileUtils.getFileSize(path),
                    rowCount,
                    minKey,
                    keySerializer.toBinaryRow(maxKey).copy(),
                    stats,
                    minSequenceNumber,
                    maxSequenceNumber,
                    level);
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

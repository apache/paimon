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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactRewriter;
import org.apache.flink.table.store.file.data.AppendOnlyCompactManager;
import org.apache.flink.table.store.file.data.AppendOnlyCompactStrategy;
import org.apache.flink.table.store.file.data.AppendOnlyWriter;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.file.writer.MetricFileWriter;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/** {@link FileStoreWrite} for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreWrite extends AbstractFileStoreWrite<RowData> {

    private final AppendOnlyFileStoreRead read;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final long targetFileSize;
    private final boolean commitForceCompact;

    public AppendOnlyFileStoreWrite(
            AppendOnlyFileStoreRead read,
            long schemaId,
            RowType rowType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            long targetFileSize,
            boolean commitForceCompact) {
        super(snapshotManager, scan);
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = fileFormat;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;
        this.commitForceCompact = commitForceCompact;
    }

    @Override
    public RecordWriter<RowData> createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createWriter(
                partition, bucket, scanExistingFileMetas(partition, bucket), compactExecutor);
    }

    @Override
    public RecordWriter<RowData> createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createWriter(partition, bucket, Collections.emptyList(), compactExecutor);
    }

    @Override
    public Callable<CompactResult> createCompactWriter(
            BinaryRowData partition, int bucket, @Nullable List<DataFileMeta> compactFiles) {
        throw new UnsupportedOperationException(
                "Currently append only write mode does not support compaction.");
    }

    private RecordWriter<RowData> createWriter(
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor) {
        DataFilePathFactory factory = pathFactory.createDataFilePathFactory(partition, bucket);
        FileWriter.Factory<RowData, Metric> fileWriterFactory =
                MetricFileWriter.createFactory(
                        fileFormat.createWriterFactory(rowType),
                        Function.identity(),
                        rowType,
                        fileFormat.createStatsExtractor(rowType).orElse(null));
        FieldStatsArraySerializer statsArraySerializer = new FieldStatsArraySerializer(rowType);
        return new AppendOnlyWriter(
                schemaId,
                targetFileSize,
                fileWriterFactory,
                statsArraySerializer,
                createCompactManager(
                        compactExecutor,
                        createCompactRewriter(
                                partition, bucket, fileWriterFactory, statsArraySerializer)),
                commitForceCompact,
                restoredFiles,
                getMaxSequenceNumber(restoredFiles),
                factory);
    }

    private AppendOnlyCompactManager createCompactManager(
            ExecutorService compactExecutor, CompactRewriter<DataFileMeta> rewriter) {
        return new AppendOnlyCompactManager(
                compactExecutor, new AppendOnlyCompactStrategy(targetFileSize), rewriter);
    }

    private CompactRewriter<DataFileMeta> createCompactRewriter(
            BinaryRowData partition,
            int bucket,
            FileWriter.Factory<RowData, Metric> fileWriterFactory,
            FieldStatsArraySerializer statsArraySerializer) {
        return (outputLevel, dropDelete, sections) -> {
            List<DataFileMeta> results = new ArrayList<>();
            for (List<DataFileMeta> section : sections) {
                AppendOnlyWriter.SingleFileWriter writer =
                        createRewriter(
                                partition,
                                bucket,
                                fileWriterFactory,
                                statsArraySerializer,
                                section);
                results.add(
                        writeIterator(
                                writer,
                                new RecordReaderIterator<>(
                                        read.createReader(
                                                new Split(partition, bucket, section, false)))));
            }
            return results;
        };
    }

    private AppendOnlyWriter.SingleFileWriter createRewriter(
            BinaryRowData partition,
            int bucket,
            FileWriter.Factory<RowData, Metric> fileWriterFactory,
            FieldStatsArraySerializer statsArraySerializer,
            List<DataFileMeta> section) {
        return new AppendOnlyWriter.SingleFileWriter(
                fileWriterFactory,
                pathFactory.createDataFilePathFactory(partition, bucket).newPath(),
                statsArraySerializer,
                section.get(0).minSequenceNumber(),
                section.get(section.size() - 1).maxSequenceNumber(),
                schemaId);
    }

    private DataFileMeta writeIterator(
            AppendOnlyWriter.SingleFileWriter writer, CloseableIterator<RowData> iterator)
            throws Exception {
        try {
            writer.write(iterator);
            writer.close();
            return writer.result();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            iterator.close();
        }
    }
}

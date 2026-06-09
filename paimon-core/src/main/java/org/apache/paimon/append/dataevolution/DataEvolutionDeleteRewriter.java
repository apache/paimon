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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.StatsCollectorFactories;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongFunction;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/** Rewrites data-evolution files by removing deleted row id ranges. */
public class DataEvolutionDeleteRewriter {

    private final FileStoreTable table;
    private final CoreOptions options;
    private final AppendOnlyFileStore store;
    private final FileStorePathFactory pathFactory;
    private final FileFormatDiscover formatDiscover;
    private final StatsCollectorFactories statsCollectorFactories;
    private final LongFunction<TableSchema> schemaFetcher;
    private final Map<Long, TableSchema> schemaCache;

    public DataEvolutionDeleteRewriter(FileStoreTable table) {
        this.table = table;
        this.options = table.coreOptions();
        this.store = (AppendOnlyFileStore) table.store();
        this.pathFactory = table.store().pathFactory();
        this.formatDiscover = FileFormatDiscover.of(options);
        this.statsCollectorFactories = new StatsCollectorFactories(options);
        this.schemaCache = new LinkedHashMap<>();
        this.schemaFetcher =
                schemaId -> schemaCache.computeIfAbsent(schemaId, table.schemaManager()::schema);
    }

    public List<CommitMessage> rewrite(List<DataSplit> dataSplits, List<Range> deleteRanges)
            throws IOException {
        if (deleteRanges.isEmpty()) {
            return Collections.emptyList();
        }

        Map<SplitKey, RewritePlan> plans = new LinkedHashMap<>();
        for (DataSplit dataSplit : dataSplits) {
            SplitKey key =
                    new SplitKey(
                            dataSplit.partition(), dataSplit.bucket(), dataSplit.totalBuckets());
            RewritePlan plan = plans.computeIfAbsent(key, k -> new RewritePlan());
            for (DataFileMeta file : dataSplit.dataFiles()) {
                List<Range> retainedRanges = retainedRanges(file, deleteRanges);
                if (isFullRange(file, retainedRanges)) {
                    continue;
                }
                if (isVectorStoreFile(file.fileName())) {
                    throw new UnsupportedOperationException(
                            "DataEvolution MergeInto DELETE does not support vector-store files.");
                }

                plan.compactBefore.add(file);
                for (Range retainedRange : retainedRanges) {
                    plan.compactAfter.add(
                            rewriteFile(
                                    dataSplit.partition(),
                                    dataSplit.bucket(),
                                    file,
                                    retainedRange));
                }
            }
        }

        List<CommitMessage> result = new ArrayList<>();
        for (Map.Entry<SplitKey, RewritePlan> entry : plans.entrySet()) {
            RewritePlan plan = entry.getValue();
            if (!plan.compactBefore.isEmpty()) {
                SplitKey key = entry.getKey();
                result.add(
                        new CommitMessageImpl(
                                key.partition,
                                key.bucket,
                                key.totalBuckets,
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        plan.compactBefore,
                                        plan.compactAfter,
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList())));
            }
        }
        return result;
    }

    public static List<Range> retainedRanges(DataFileMeta file, List<Range> deleteRanges) {
        List<Range> retained = new ArrayList<>();
        retained.add(file.nonNullRowIdRange());

        for (Range deleteRange : deleteRanges) {
            List<Range> next = new ArrayList<>();
            for (Range range : retained) {
                Range intersection = Range.intersection(range, deleteRange);
                if (intersection == null) {
                    next.add(range);
                    continue;
                }
                if (range.from < intersection.from) {
                    next.add(new Range(range.from, intersection.from - 1));
                }
                if (intersection.to < range.to) {
                    next.add(new Range(intersection.to + 1, range.to));
                }
            }
            retained = next;
            if (retained.isEmpty()) {
                break;
            }
        }

        return retained;
    }

    private boolean isFullRange(DataFileMeta file, List<Range> retainedRanges) {
        return retainedRanges.size() == 1 && retainedRanges.get(0).equals(file.nonNullRowIdRange());
    }

    private DataFileMeta rewriteFile(
            BinaryRow partition, int bucket, DataFileMeta file, Range retainedRange)
            throws IOException {
        RowType writeType =
                schemaFetcher.apply(file.schemaId()).project(file.writeCols()).logicalRowType();
        DataSplit split =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withDataFiles(Collections.singletonList(file))
                        .withBucketPath(pathFactory.bucketPath(partition, bucket).toString())
                        .rawConvertible(false)
                        .build();

        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead()
                        .withReadType(writeType)
                        .createReader(
                                new IndexedSplit(
                                        split, Collections.singletonList(retainedRange), null));
        FileWriter<InternalRow, DataFileMeta> writer =
                createFileWriter(partition, bucket, file, writeType);

        try {
            reader.forEachRemaining(
                    row -> {
                        try {
                            writer.write(row);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
            writer.close();
            return writer.result()
                    .assignFirstRowId(retainedRange.from)
                    .assignSequenceNumber(file.minSequenceNumber(), file.maxSequenceNumber());
        } catch (UncheckedIOException e) {
            writer.abort();
            throw e.getCause();
        } catch (IOException e) {
            writer.abort();
            throw e;
        } catch (RuntimeException e) {
            writer.abort();
            throw e;
        }
    }

    private FileWriter<InternalRow, DataFileMeta> createFileWriter(
            BinaryRow partition, int bucket, DataFileMeta file, RowType writeType) {
        DataFilePathFactory filePathFactory =
                pathFactory.createDataFilePathFactory(partition, bucket);
        if (isBlobFile(file.fileName())) {
            return new RowDataFileWriter(
                    table.fileIO(),
                    RollingFileWriter.createFileWriterContext(
                            new BlobFileFormat(),
                            writeType,
                            noneStatsCollectors(writeType),
                            "none"),
                    filePathFactory.newBlobPath(),
                    writeType,
                    file.schemaId(),
                    () -> new LongCounter(0),
                    new FileIndexOptions(),
                    FileSource.COMPACT,
                    false,
                    options.statsDenseStore(),
                    filePathFactory.isExternalPath(),
                    file.writeCols());
        }

        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        FileFormat fileFormat = formatDiscover.discover(formatIdentifier);
        return new RowDataFileWriter(
                table.fileIO(),
                RollingFileWriter.createFileWriterContext(
                        fileFormat,
                        writeType,
                        statsCollectorFactories.statsCollectors(writeType.getFieldNames()),
                        options.fileCompression()),
                filePathFactory.newPath(),
                writeType,
                file.schemaId(),
                () -> new LongCounter(0),
                new FileIndexOptions(),
                FileSource.COMPACT,
                false,
                options.statsDenseStore(),
                filePathFactory.isExternalPath(),
                file.writeCols());
    }

    private SimpleColStatsCollector.Factory[] noneStatsCollectors(RowType writeType) {
        SimpleColStatsCollector.Factory[] factories =
                new SimpleColStatsCollector.Factory[writeType.getFieldCount()];
        for (int i = 0; i < factories.length; i++) {
            factories[i] = NoneSimpleColStatsCollector::new;
        }
        return factories;
    }

    private static class RewritePlan {

        private final List<DataFileMeta> compactBefore = new ArrayList<>();
        private final List<DataFileMeta> compactAfter = new ArrayList<>();
    }

    private static class SplitKey {

        private final BinaryRow partition;
        private final int bucket;
        private final Integer totalBuckets;

        private SplitKey(BinaryRow partition, int bucket, Integer totalBuckets) {
            this.partition = partition;
            this.bucket = bucket;
            this.totalBuckets = totalBuckets;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SplitKey)) {
                return false;
            }
            SplitKey that = (SplitKey) obj;
            return bucket == that.bucket
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(totalBuckets, that.totalBuckets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, totalBuckets);
        }
    }
}

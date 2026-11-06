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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.index.DeleteOrderingGuardSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Default {@link CompactRewriter} for merge trees. */
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {

    protected final FileReaderFactory<KeyValue> readerFactory;
    protected final KeyValueFileWriterFactory writerFactory;
    protected final Comparator<InternalRow> keyComparator;
    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    protected final MergeFunctionFactory<KeyValue> mfFactory;
    protected final MergeSorter mergeSorter;
    @Nullable private CompactionMetrics.Reporter metricsReporter;
    @Nullable private DeleteOrderingGuardCompactRewriter guardCapture;
    @Nullable private DeleteOrderingGuardState guardState;

    public MergeTreeCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.userDefinedSeqComparator = userDefinedSeqComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter = mergeSorter;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(outputLevel, dropDelete, sections);
    }

    protected CompactResult rewriteCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
        RecordReader<KeyValue> reader = null;
        Exception collectedExceptions = null;
        try {
            reader =
                    readerForMergeTree(
                            sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }
            writer.write(new RecordReaderIterator<>(reader));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }

        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before);
        List<DataFileMeta> after = writer.result();
        after = notifyRewriteCompactAfter(after);
        if (metricsReporter != null) {
            metricsReporter.reportSortBufferMetrics(
                    mergeSorter.sortBufferUsedBytes(), mergeSorter.sortBufferTotalBytes());
        }
        return new CompactResult(before, after);
    }

    protected <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections, MergeFunctionWrapper<T> mergeFunctionWrapper)
            throws IOException {
        if (guardCapture != null) {
            mergeFunctionWrapper = guardCapture.wrap(mergeFunctionWrapper);
        }
        DeleteOrderingGuardReader guards =
                guardState == null || guardState.isEmpty() ? null : guardState.open();
        try {
            if (guards != null) {
                mergeFunctionWrapper = guards.wrap(mergeFunctionWrapper, userDefinedSeqComparator);
            }
            RecordReader<T> reader =
                    MergeTreeReaders.readerForMergeTree(
                            sections,
                            readerFactory,
                            keyComparator,
                            userDefinedSeqComparator,
                            mergeFunctionWrapper,
                            mergeSorter);
            return guards == null ? reader : guards.withDataReader(reader);
        } catch (IOException | RuntimeException | Error failure) {
            if (guards != null) {
                try {
                    guards.close();
                } catch (Throwable cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    public void withDeleteOrderingGuardState(DeleteOrderingGuardState guardState) {
        this.guardState = guardState;
    }

    @Override
    public void release(CompactResult result) {
        if (guardState != null) {
            guardState.remove(result.deletedIndexFiles());
            guardState.add(result.newIndexFiles());
        }
    }

    /**
     * Installs initial capture internally. Returned references must be committed with the
     * source-file replacements. Bucket factories supply persisted guards for subsequent reads and
     * compactions; ordinary table options do not enable initial capture.
     */
    public CompactRewriter withDeleteOrderingGuards(
            DeleteOrderingGuardSerializer serializer, DeleteOrderingGuardFile guardFile) {
        checkState(guardCapture == null, "Delete ordering guard capture is already installed.");
        guardCapture = new DeleteOrderingGuardCompactRewriter(this, serializer, guardFile);
        return guardCapture;
    }

    CompactRewriter ensureDeleteOrderingGuards(
            CompactRewriter rewriter,
            DeleteOrderingGuardSerializer serializer,
            DeleteOrderingGuardFile guardFile) {
        if (guardCapture == null) {
            guardCapture = new DeleteOrderingGuardCompactRewriter(rewriter, serializer, guardFile);
            return guardCapture;
        }
        return rewriter;
    }

    @Override
    public void abort(CompactResult result) {
        Set<String> before =
                result.before().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        result.after().stream()
                .filter(file -> !before.contains(file.fileName()))
                .forEach(writerFactory::deleteFile);
        result.changelog().forEach(writerFactory::deleteFile);
        result.newIndexFiles().forEach(writerFactory::deleteIndexFile);
    }

    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}

    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        return files;
    }

    public void setMetricsReporter(@Nullable CompactionMetrics.Reporter reporter) {
        this.metricsReporter = reporter;
    }
}

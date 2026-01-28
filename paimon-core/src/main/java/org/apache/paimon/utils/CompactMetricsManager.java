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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.metrics.CompactMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Manager for {@link CompactMetric}, providing utility methods related to paths and compact metrics
 * hints.
 */
public class CompactMetricsManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CompactMetricsManager.class);

    public static final String COMPACTION_METRICS_PREFIX = "compact-";

    public static final int EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM = 3;

    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;

    public CompactMetricsManager(FileIO fileIO, Path tablePath, @Nullable String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path tablePath() {
        return tablePath;
    }

    public String branch() {
        return branch;
    }

    public Path compactMetricPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch)
                        + "/metrics/"
                        + COMPACTION_METRICS_PREFIX
                        + snapshotId);
    }

    public Path compactMetricsDirectory() {
        return new Path(branchPath(tablePath, branch) + "/metrics");
    }

    public CompactMetric metric(long snapshotId) {
        Path path = compactMetricPath(snapshotId);
        return fromPath(fileIO, path);
    }

    public boolean commit(CompactMetric metric) throws Exception {
        Path metricPath = compactMetricPath(metric.snapshotId());
        return fileIO.tryToWriteAtomic(metricPath, metric.toJson());
    }

    public CompactMetric tryGetCompactMetric(long snapshotId) throws FileNotFoundException {
        Path path = compactMetricPath(snapshotId);
        return tryFromPath(fileIO, path);
    }

    public boolean compactMetricExists(long snapshotId) {
        Path path = compactMetricPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public void deleteCompactMetric(long snapshotId) {
        Path path = compactMetricPath(snapshotId);
        fileIO().deleteQuietly(path);
    }

    public long compactMetricCount() throws IOException {
        return metricIdStream().count();
    }

    public List<Long> compactMetricIds() throws IOException {
        return metricIdStream().filter(Objects::nonNull).sorted().collect(Collectors.toList());
    }

    public Iterator<CompactMetric> metrics() throws IOException {
        return metricIdStream()
                .map(this::metric)
                .sorted(Comparator.comparingLong(CompactMetric::snapshotId))
                .iterator();
    }

    public List<Path> metricsPaths(Predicate<Long> predicate) throws IOException {
        return metricIdStream()
                .filter(predicate)
                .map(this::compactMetricPath)
                .collect(Collectors.toList());
    }

    public Stream<Long> metricIdStream() throws IOException {
        return listVersionedFiles(fileIO, compactMetricsDirectory(), COMPACTION_METRICS_PREFIX);
    }

    public Iterator<CompactMetric> metricsWithId(List<Long> snapshotIds) {
        return snapshotIds.stream()
                .map(this::metric)
                .sorted(Comparator.comparingLong(CompactMetric::snapshotId))
                .iterator();
    }

    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest(
                    compactMetricsDirectory(), COMPACTION_METRICS_PREFIX, this::compactMetricPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest compaction metrics snapshot id", e);
        }
    }

    public @Nullable Long latestSnapshotId() {
        try {
            return findLatest(
                    compactMetricsDirectory(), COMPACTION_METRICS_PREFIX, this::compactMetricPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest compaction metrics snapshot id", e);
        }
    }

    /**
     * If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot may
     * be deleted by other processes, so just skip this snapshot.
     */
    public List<CompactMetric> safelyGetAllmetrics() throws IOException {
        List<Path> paths =
                metricIdStream().map(this::compactMetricPath).collect(Collectors.toList());

        List<CompactMetric> metrics = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectMetrics(
                path -> {
                    try {
                        // do not pollution cache
                        metrics.add(tryFromPath(fileIO, path));
                    } catch (FileNotFoundException ignored) {
                    }
                },
                paths);

        return metrics;
    }

    private static void collectMetrics(Consumer<Path> pathConsumer, List<Path> paths)
            throws IOException {
        ExecutorService executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "COMPACTION_METRICS_COLLECTOR");

        try {
            randomlyOnlyExecute(executor, pathConsumer, paths);
        } catch (RuntimeException e) {
            throw new IOException(e);
        } finally {
            executor.shutdown();
        }
    }

    private @Nullable Long findLatest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        return HintFileUtils.findLatest(fileIO, dir, prefix, file);
    }

    private @Nullable Long findEarliest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        return HintFileUtils.findEarliest(fileIO, dir, prefix, file);
    }

    public void deleteLatestHint() throws IOException {
        HintFileUtils.deleteLatestHint(fileIO, compactMetricsDirectory());
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        HintFileUtils.commitLatestHint(fileIO, snapshotId, compactMetricsDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        HintFileUtils.commitEarliestHint(fileIO, snapshotId, compactMetricsDirectory());
    }

    public static CompactMetric fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            String errorMessage =
                    String.format(
                            "Compaction Metric file %s does not exist. "
                                    + "It might have been expired by other jobs operating on this table. "
                                    + "In this case, you can avoid concurrent modification issues by configuring "
                                    + "write-only = true and use a dedicated compaction job, or configuring "
                                    + "different expiration thresholds for different jobs.",
                            path);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public static CompactMetric tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 10) {
            String content;
            try {
                content = fileIO.readFileUtf8(path);
            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                throw new RuntimeException("Fails to read snapshot from path " + path, e);
            }

            try {
                return CompactMetric.fromJson(content);
            } catch (Exception e) {
                // retry
                exception = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        throw new RuntimeException("Retry fail after 10 times", exception);
    }
}

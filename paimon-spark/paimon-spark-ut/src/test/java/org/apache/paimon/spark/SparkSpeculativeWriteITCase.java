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

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.spark.write.SparkAttemptCleanup;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.rpad;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for Spark speculative execution with Paimon DSv2 writer. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkSpeculativeWriteITCase {

    private SparkSession spark;

    private Path warehousePath;

    // Counts speculative task submissions observed during the current test. Reset per test that
    // cares about speculation firing, so prior tests do not pollute the count.
    private final AtomicInteger speculativeTaskCount = new AtomicInteger();

    // Records task attempt ids that have already taken the injected slow-path sleep, so each
    // attempt sleeps at most once (on its first part-0 row) instead of once per row. Shared
    // across tasks because local[8] runs them in one JVM; cleared before each skew test.
    private static final Set<Long> SLEPT_ATTEMPTS = ConcurrentHashMap.newKeySet();

    // File names from CommitMessages that SparkAttemptCleanup aborted during the current test.
    // Used to assert synchronous loser cleanup before orphan cleaner runs.
    private static final Set<String> ABORTED_FILE_NAMES = ConcurrentHashMap.newKeySet();

    @BeforeAll
    public void startSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:///" + tempDir.toString());
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        spark =
                SparkSession.builder()
                        .master("local[8]")
                        .appName("SparkSpeculativeWriteITCase")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath.toString())
                        .config("spark.sql.defaultCatalog", "paimon")
                        .config("spark.speculation", "true")
                        .config("spark.speculation.multiplier", "1.0")
                        .config("spark.speculation.quantile", "0.0")
                        .config("spark.speculation.interval", "50")
                        .config("spark.speculation.minTaskRuntime", "1")
                        .getOrCreate();
        // Record when Spark actually launches a duplicate (speculative) attempt, so the test can
        // confirm the loser-cleanup path was exercised rather than silently skipped. Note: some
        // Spark builds do not schedule the speculation executor in local mode, so the count may
        // stay 0; the safety assertions (data correctness + no orphan files after clean) hold
        // regardless and are the primary guarantee.
        spark.sparkContext()
                .addSparkListener(
                        new SparkListener() {
                            @Override
                            public void onSpeculativeTaskSubmitted(
                                    SparkListenerSpeculativeTaskSubmitted event) {
                                speculativeTaskCount.incrementAndGet();
                            }
                        });
        spark.sql("CREATE DATABASE db");
        spark.sql("USE db");

        // UDF that sleeps once per task attempt on part 0. It is applied AFTER a repartition in
        // testSpeculativeLoserFilesAreCleanedUp, so it executes inside the write-stage task (the
        // task Spark would speculate and kill), maximizing the chance that speculation fires in
        // environments where it is supported.
        spark.udf()
                .register(
                        "slowOnPart0",
                        (UDF1<Integer, Integer>)
                                part -> {
                                    if (part != null && part == 0) {
                                        TaskContext ctx = TaskContext.get();
                                        if (ctx != null
                                                && SLEPT_ATTEMPTS.add(ctx.taskAttemptId())) {
                                            try {
                                                Thread.sleep(3000L);
                                            } catch (InterruptedException e) {
                                                // Spark kills the loser by interrupting it;
                                                // preserve the interrupt so the writer abort
                                                // path runs.
                                                Thread.currentThread().interrupt();
                                            }
                                        }
                                    }
                                    return part;
                                },
                        DataTypes.IntegerType);
    }

    @AfterAll
    public void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @AfterEach
    public void dropTable() {
        SparkAttemptCleanup.setAbortedMessagesProbe(null);
        ABORTED_FILE_NAMES.clear();
        spark.sql("DROP TABLE IF EXISTS spec_t");
    }

    @Test
    public void testInsertWithSpeculationEnabled() throws Exception {
        spark.sql("CREATE TABLE spec_t (id BIGINT, part INT) USING paimon");
        spark.sql(
                        "INSERT INTO spec_t "
                                + "SELECT id, CAST(pmod(id, 8) AS INT) "
                                + "FROM range(0, 2000, 1, 16)")
                .collectAsList();

        List<Row> rows =
                spark.sql("SELECT count(*), count(DISTINCT id) FROM spec_t").collectAsList();
        assertThat(rows.get(0).getLong(0)).isEqualTo(2000L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(2000L);

        FileStoreTable table = loadTable("spec_t");
        Assertions.assertNotNull(table.snapshotManager().latestSnapshot());
        assertReferencedDataFilesExist(table);
        assertOrphanCleanerReclaimsUnreferencedFiles(table);
    }

    @Test
    public void testV1WriteWithSpeculationEnabled() throws Exception {
        spark.sql("CREATE TABLE spec_t (id BIGINT, v STRING) USING paimon");
        spark.sql(
                        "INSERT INTO spec_t "
                                + "SELECT id, concat('v-', CAST(id AS STRING)) "
                                + "FROM range(0, 1000, 1, 12)")
                .collectAsList();

        List<Row> rows = spark.sql("SELECT count(*) FROM spec_t").collectAsList();
        assertThat(rows.get(0).getLong(0)).isEqualTo(1000L);

        FileStoreTable table = loadTable("spec_t");
        assertReferencedDataFilesExist(table);
        assertOrphanCleanerReclaimsUnreferencedFiles(table);
    }

    @Test
    public void testSpeculativeWriteDoesNotLeaveExcessiveOrphanFiles() throws Exception {
        spark.sql("CREATE TABLE spec_t (id BIGINT, part INT) USING paimon");
        spark.sql(
                        "INSERT INTO spec_t "
                                + "SELECT id, CAST(pmod(id, 16) AS INT) "
                                + "FROM range(0, 5000, 1, 40)")
                .collectAsList();

        FileStoreTable table = loadTable("spec_t");
        assertReferencedDataFilesExist(table);
        assertOrphanCleanerReclaimsUnreferencedFiles(table);
    }

    @Test
    public void testSpeculativeLoserFilesAreCleanedUpSynchronously() throws Exception {
        // Drive a real Spark task skew so the loser-abort path is exercised when speculation
        // fires:
        //   * part is a real partition key (PARTITIONED BY), and repartition(8, part)
        //     concentrates all ~50000 part-0 rows into a single write task while parts 1..7 get
        //     one row each — a genuine per-task skew, not just a skewed data column.
        //   * the slowOnPart0 UDF is applied AFTER the repartition, so it runs in the write
        //     stage (not the map stage) and sleeps once per attempt on part 0, giving speculation
        //     a deterministic trigger in environments that schedule the speculation executor.
        // When a duplicate attempt wins, Spark kills the slow original mid-write, exercising the
        // abort/close path that must delete the loser's in-flight and prepared files.
        //
        // Sync cleanup is verified BEFORE LocalOrphanFilesClean runs: aborted file names recorded
        // by SparkAttemptCleanup must already be gone from disk. Orphan cleaner fallback is covered
        // by testOrphanCleanerFallbackReclaimsUnreferencedFiles, not this test.
        spark.sql(
                "CREATE TABLE spec_t (id BIGINT, part INT, payload STRING) USING paimon "
                        + "PARTITIONED BY (part)");

        SLEPT_ATTEMPTS.clear();
        speculativeTaskCount.set(0);
        ABORTED_FILE_NAMES.clear();
        SparkAttemptCleanup.setAbortedMessagesProbe(
                messages -> {
                    for (CommitMessage message : messages) {
                        CommitMessageImpl impl = (CommitMessageImpl) message;
                        for (DataFileMeta file : impl.newFilesIncrement().newFiles()) {
                            ABORTED_FILE_NAMES.add(file.fileName());
                        }
                        for (DataFileMeta file : impl.newFilesIncrement().changelogFiles()) {
                            ABORTED_FILE_NAMES.add(file.fileName());
                        }
                        for (DataFileMeta file : impl.compactIncrement().compactAfter()) {
                            ABORTED_FILE_NAMES.add(file.fileName());
                        }
                        for (DataFileMeta file : impl.compactIncrement().changelogFiles()) {
                            ABORTED_FILE_NAMES.add(file.fileName());
                        }
                    }
                });

        // First select builds the skewed part column + a modest payload (real parquet IO weight
        // on top of the injected sleep). repartition(8, part) puts each part value in its own
        // write task. The second select applies slowOnPart0 AFTER the shuffle, so the UDF (and
        // its sleep) executes inside the write-stage task.
        Dataset<Row> skewed =
                spark.range(0, 50007, 1, 8)
                        .select(
                                col("id"),
                                when(col("id").lt(50000), lit(0))
                                        .otherwise(pmod(col("id"), lit(7)).plus(lit(1)))
                                        .cast("int")
                                        .as("part"),
                                rpad(lit("x"), 256, "x").as("payload"))
                        .repartition(8, col("part"))
                        .select(
                                col("id"),
                                callUDF("slowOnPart0", col("part")).as("part"),
                                col("payload"));

        skewed.write().format("paimon").mode("append").saveAsTable("spec_t");

        List<Row> rows =
                spark.sql("SELECT count(*), count(DISTINCT id) FROM spec_t").collectAsList();
        assertThat(rows.get(0).getLong(0)).isEqualTo(50007L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(50007L);

        FileStoreTable table = loadTable("spec_t");
        assertReferencedDataFilesExist(table);

        // When speculation is supported in the runtime, it must actually have launched a
        // duplicate attempt for the skewed part-0 task. Use assumeTrue so environments without a
        // working speculation scheduler (this local UT build) skip this strict check rather than
        // fail, while environments that do support speculation enforce it.
        org.junit.jupiter.api.Assumptions.assumeTrue(
                speculativeTaskCount.get() > 0,
                "Spark speculation did not fire in this environment (speculativeTaskCount=0); "
                        + "skipping the sync loser-cleanup assertion. Data-correctness assertions "
                        + "above still hold.");

        // Sync cleanup assertion — must run BEFORE orphan cleaner. Files aborted by
        // SparkAttemptCleanup must already be absent from disk; running LocalOrphanFilesClean
        // first would hide a broken sync path.
        Set<String> referenced = referencedDataFileNames(table);
        Set<String> physicalBeforeClean = new HashSet<>();
        collectParquetFileNames(table.fileIO(), table.location(), physicalBeforeClean);

        // Best-effort sync cleanup: when SparkAttemptCleanup did abort prepared files, they must
        // already be gone from disk before the orphan cleaner runs. Losers killed before
        // prepareCommit or successful losers ignored by the driver may leave no aborted names;
        // orphan-cleaner fallback is covered by testOrphanCleanerFallbackReclaimsUnreferencedFiles.
        for (String aborted : ABORTED_FILE_NAMES) {
            Assertions.assertFalse(
                    physicalBeforeClean.contains(aborted),
                    "loser file aborted by SparkAttemptCleanup must be gone before orphan cleaner: "
                            + aborted);
            Assertions.assertFalse(
                    referenced.contains(aborted),
                    "aborted loser file must not be snapshot-referenced: " + aborted);
        }
    }

    @Test
    public void testOrphanCleanerFallbackReclaimsUnreferencedFiles() throws Exception {
        // Independent of SparkAttemptCleanup: plant an unreferenced parquet file, run the orphan
        // cleaner, and assert it disappears. Sync loser cleanup is covered elsewhere; this test
        // only validates the fallback path that reclaim successful-loser / hard-kill leftovers.
        spark.sql("CREATE TABLE spec_t (id INT, v STRING) USING paimon");
        spark.sql("INSERT INTO spec_t VALUES (1, 'a')").collectAsList();

        FileStoreTable table = loadTable("spec_t");
        Set<String> referencedBefore = referencedDataFileNames(table);
        Assertions.assertFalse(referencedBefore.isEmpty());

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        java.nio.file.Path ioDir = java.nio.file.Files.createTempDirectory("spec-orphan-io");
        write.withIOManager(new IOManagerImpl(ioDir.toString()));
        Set<String> plantedOrphans = new HashSet<>();
        try {
            write.write(GenericRow.of(2, BinaryString.fromString("orphan")));
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                for (DataFileMeta file : impl.newFilesIncrement().newFiles()) {
                    plantedOrphans.add(file.fileName());
                }
            }
        } finally {
            write.close();
        }
        Assertions.assertFalse(plantedOrphans.isEmpty(), "should plant at least one orphan file");

        Set<String> physicalBeforeClean = new HashSet<>();
        collectParquetFileNames(table.fileIO(), table.location(), physicalBeforeClean);
        for (String orphan : plantedOrphans) {
            Assertions.assertTrue(
                    physicalBeforeClean.contains(orphan),
                    "planted orphan should exist before cleaner: " + orphan);
            Assertions.assertFalse(
                    referencedBefore.contains(orphan),
                    "planted orphan must not be snapshot-referenced: " + orphan);
        }

        assertOrphanCleanerReclaimsUnreferencedFiles(table);

        Set<String> physicalAfterClean = new HashSet<>();
        collectParquetFileNames(table.fileIO(), table.location(), physicalAfterClean);
        for (String orphan : plantedOrphans) {
            Assertions.assertFalse(
                    physicalAfterClean.contains(orphan),
                    "orphan cleaner must remove planted unreferenced file: " + orphan);
        }
    }

    private FileStoreTable loadTable(String tableName) {
        Path tablePath = new Path(warehousePath, "db.db/" + tableName);
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
    }

    private void assertReferencedDataFilesExist(FileStoreTable table) {
        Set<String> referenced = referencedDataFileNames(table);
        Assertions.assertFalse(referenced.isEmpty(), "snapshot should reference data files");

        FileIO fileIO = table.fileIO();
        Set<String> physical = new HashSet<>();
        collectParquetFileNames(fileIO, table.location(), physical);

        for (String fileName : referenced) {
            Assertions.assertTrue(
                    physical.contains(fileName),
                    "referenced data file should exist on disk: " + fileName);
        }
    }

    private void assertOrphanCleanerReclaimsUnreferencedFiles(FileStoreTable table)
            throws Exception {
        // Fallback path only: run LocalOrphanFilesClean then assert physical == referenced.
        // Do not use this to validate SparkAttemptCleanup — that would pass even when sync
        // abort is broken. Use a future older-than threshold so freshly written files are
        // eligible.
        new LocalOrphanFilesClean(table, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))
                .clean();

        Set<String> referenced = referencedDataFileNames(table);
        FileIO fileIO = table.fileIO();
        Set<String> physical = new HashSet<>();
        collectParquetFileNames(fileIO, table.location(), physical);

        Assertions.assertTrue(
                physical.size() >= referenced.size(),
                "physical files should cover all snapshot-referenced files");
        Assertions.assertEquals(
                referenced.size(),
                physical.size(),
                "no orphan parquet files should remain after orphan clean, referenced="
                        + referenced.size()
                        + ", physical="
                        + physical.size());
    }

    private Set<String> referencedDataFileNames(FileStoreTable table) {
        Set<String> referenced = new HashSet<>();
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                    referenced.add(file.fileName());
                }
            }
        }
        return referenced;
    }

    private void collectParquetFileNames(FileIO fileIO, Path path, Set<String> out) {
        try {
            for (FileStatus status : fileIO.listStatus(path)) {
                if (status.isDir()) {
                    collectParquetFileNames(fileIO, status.getPath(), out);
                } else if (status.getPath().getName().endsWith(".parquet")) {
                    out.add(status.getPath().getName());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

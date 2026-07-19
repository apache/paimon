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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.spark.SparkProcedures;
import org.apache.paimon.spark.format.FormatTablePartitionCatalog;
import org.apache.paimon.spark.format.FormatTablePartitionPage;
import org.apache.paimon.spark.format.PaimonFormatTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for managed format-table metadata procedures. */
class FormatTableMetadataProcedureTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void procedureImplementationsAreAvailable() {
        assertThatCode(
                        () ->
                                Class.forName(
                                        "org.apache.paimon.spark.procedure.ListFormatTablePartitionsProcedure"))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                Class.forName(
                                        "org.apache.paimon.spark.procedure.SyncFormatTableMetadataProcedure"))
                .doesNotThrowAnyException();
    }

    @Test
    void implementationsExposeProcedureBuilders() throws Exception {
        assertThat(Procedure.class).isAssignableFrom(ListFormatTablePartitionsProcedure.class);
        assertThat(Procedure.class).isAssignableFrom(SyncFormatTableMetadataProcedure.class);
        assertThat(
                        ListFormatTablePartitionsProcedure.class
                                .getDeclaredMethod("builder")
                                .getReturnType())
                .isEqualTo(ProcedureBuilder.class);
        assertThat(SparkProcedures.names())
                .contains("list_format_table_partitions", "sync_format_table_metadata");
        assertThat(
                        SyncFormatTableMetadataProcedure.class
                                .getDeclaredMethod("builder")
                                .getReturnType())
                .isEqualTo(ProcedureBuilder.class);
    }

    @Test
    void listProcedureDeclaresPagedPredicateContract() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("list-format-table-partitions-procedure-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            Procedure procedure =
                    ListFormatTablePartitionsProcedure.builder().withTableCatalog(null).build();

            assertThat(
                            Arrays.stream(procedure.parameters())
                                    .map(ProcedureParameter::name)
                                    .collect(Collectors.toList()))
                    .containsExactly("table", "where", "limit", "page_token");
            assertThat(
                            Arrays.stream(procedure.parameters())
                                    .map(ProcedureParameter::required)
                                    .collect(Collectors.toList()))
                    .containsExactly(true, false, false, false);
            assertThat(procedure.outputType().fieldNames())
                    .containsExactly("partition", "next_page_token");
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncProcedureDeclaresSafeRepairContract() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-procedure-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            Procedure procedure =
                    SyncFormatTableMetadataProcedure.builder().withTableCatalog(null).build();

            assertThat(
                            Arrays.stream(procedure.parameters())
                                    .map(ProcedureParameter::name)
                                    .collect(Collectors.toList()))
                    .containsExactly("table", "mode", "dry_run");
            assertThat(
                            Arrays.stream(procedure.parameters())
                                    .map(ProcedureParameter::required)
                                    .collect(Collectors.toList()))
                    .containsExactly(true, false, false);
            assertThat(procedure.outputType().fieldNames())
                    .containsExactly("partition", "action", "status");
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncProcedureCallDefaultsToDryRunAndApplyCreatesOneBatch() throws Exception {
        java.nio.file.Path partitionDirectory =
                Files.createDirectories(tempDir.resolve("dt=20260701"));
        Files.write(
                partitionDirectory.resolve("data.csv"),
                Collections.singletonList("1"),
                StandardCharsets.UTF_8);

        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-call-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            RecordingCatalog partitionCatalog = new RecordingCatalog();
            partitionCatalog.addPage(
                    null, new FormatTablePartitionPage(Collections.emptyList(), null));
            PaimonFormatTable sparkTable =
                    new PaimonFormatTable(
                            formatTable(tempDir.toUri().toString()), partitionCatalog);
            AtomicInteger refreshCount = new AtomicInteger();
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(tableCatalog(sparkTable)) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {
                            refreshCount.incrementAndGet();
                        }
                    };

            InternalRow[] dryRunRows =
                    procedure.call(
                            new GenericInternalRow(
                                    new Object[] {UTF8String.fromString("db.t"), null, null}));

            assertThat(dryRunRows).hasSize(1);
            assertThat(dryRunRows[0].getUTF8String(0).toString()).isEqualTo("dt=20260701");
            assertThat(dryRunRows[0].getUTF8String(1).toString()).isEqualTo("ADD");
            assertThat(dryRunRows[0].getUTF8String(2).toString()).isEqualTo("DRY_RUN");
            assertThat(partitionCatalog.createdPartitions).isEmpty();
            assertThat(refreshCount).hasValue(0);

            InternalRow[] applyRows =
                    procedure.call(
                            new GenericInternalRow(
                                    new Object[] {UTF8String.fromString("db.t"), null, false}));

            assertThat(applyRows).hasSize(1);
            assertThat(applyRows[0].getUTF8String(2).toString()).isEqualTo("REGISTERED");
            assertThat(partitionCatalog.createdPartitions)
                    .containsExactly(Collections.singletonList(spec("dt", "20260701")));
            assertThat(partitionCatalog.createIgnoreFlags).containsExactly(true);
            assertThat(refreshCount).hasValue(1);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncProcedureAppliesLargeDiffInBoundedBatches() throws Exception {
        int partitionCount = 1001;
        for (int index = 0; index < partitionCount; index++) {
            Files.createDirectories(tempDir.resolve(String.format("dt=%04d", index)));
        }

        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-batch-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            RecordingCatalog partitionCatalog = new RecordingCatalog();
            partitionCatalog.addPage(
                    null, new FormatTablePartitionPage(Collections.emptyList(), null));
            PaimonFormatTable sparkTable =
                    new PaimonFormatTable(
                            formatTable(tempDir.toUri().toString()), partitionCatalog);
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(tableCatalog(sparkTable)) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {}
                    };

            InternalRow[] applyRows =
                    procedure.call(
                            new GenericInternalRow(
                                    new Object[] {UTF8String.fromString("db.t"), null, false}));

            assertThat(applyRows).hasSize(partitionCount);
            assertThat(partitionCatalog.createdPartitions).hasSize(2);
            assertThat(partitionCatalog.createdPartitions.get(0)).hasSize(1000);
            assertThat(partitionCatalog.createdPartitions.get(1)).hasSize(1);
            assertThat(partitionCatalog.createIgnoreFlags).containsExactly(true, true);
            assertThat(partitionCatalog.createdPartitions.stream().mapToInt(List::size).sum())
                    .isEqualTo(partitionCount);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void listProcedurePropagatesPartitionCatalogFailure() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("list-format-table-partitions-error-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            SecurityException failure = new SecurityException("partition access denied");
            RecordingCatalog partitionCatalog =
                    new RecordingCatalog() {
                        @Override
                        public FormatTablePartitionPage listPartitions(
                                Map<String, String> prefix, String pageToken, int pageSize) {
                            throw failure;
                        }
                    };
            Procedure procedure =
                    ListFormatTablePartitionsProcedure.builder()
                            .withTableCatalog(
                                    tableCatalog(
                                            new PaimonFormatTable(formatTable(), partitionCatalog)))
                            .build();

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"), null, 1, null
                                                    })))
                    .isSameAs(failure);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void listProcedureRejectsOutOfRangeLimit() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("list-format-table-partitions-limit-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            RecordingCatalog partitionCatalog = new RecordingCatalog();
            Procedure procedure =
                    ListFormatTablePartitionsProcedure.builder()
                            .withTableCatalog(
                                    tableCatalog(
                                            new PaimonFormatTable(formatTable(), partitionCatalog)))
                            .build();

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"), null, 0, null
                                                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("limit must be between 1 and 10000");
            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"),
                                                        null,
                                                        10001,
                                                        null
                                                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("limit must be between 1 and 10000");
            assertThat(partitionCatalog.requestedTokens).isEmpty();
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void listProcedureAppliesSqlPartitionPredicatePerCatalogPage() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("list-format-table-partitions-call-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            RecordingCatalog partitionCatalog = new RecordingCatalog();
            partitionCatalog.addPage(
                    null,
                    new FormatTablePartitionPage(
                            Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")), "p2"));
            partitionCatalog.addPage(
                    "p2",
                    new FormatTablePartitionPage(
                            Arrays.asList(spec("dt", "20260703"), spec("dt", "20260704")), null));
            PaimonFormatTable sparkTable = new PaimonFormatTable(formatTable(), partitionCatalog);
            TableCatalog tableCatalog = tableCatalog(sparkTable);
            Procedure procedure =
                    ListFormatTablePartitionsProcedure.builder()
                            .withTableCatalog(tableCatalog)
                            .build();

            // A filtered page is the matching subset of one raw catalog page: it may be sparse
            // and carries the catalog's own next-page token verbatim.
            InternalRow[] rows =
                    procedure.call(
                            new GenericInternalRow(
                                    new Object[] {
                                        UTF8String.fromString("db.t"),
                                        UTF8String.fromString("dt >= '20260702'"),
                                        2,
                                        null
                                    }));

            assertThat(rows).hasSize(1);
            assertThat(rows[0].getUTF8String(0).toString()).isEqualTo("dt=20260702");
            assertThat(rows[0].getUTF8String(1).toString()).isEqualTo("p2");
            assertThat(partitionCatalog.requestedTokens).containsExactly((String) null);

            InternalRow[] nextRows =
                    procedure.call(
                            new GenericInternalRow(
                                    new Object[] {
                                        UTF8String.fromString("db.t"),
                                        UTF8String.fromString("dt >= '20260702'"),
                                        2,
                                        UTF8String.fromString("p2")
                                    }));

            assertThat(nextRows).hasSize(2);
            assertThat(nextRows[0].getUTF8String(0).toString()).isEqualTo("dt=20260703");
            assertThat(nextRows[1].getUTF8String(0).toString()).isEqualTo("dt=20260704");
            assertThat(nextRows[0].isNullAt(1)).isTrue();
            assertThat(nextRows[1].isNullAt(1)).isTrue();
            assertThat(partitionCatalog.requestedTokens).containsExactly(null, "p2");
            assertThat(partitionCatalog.requestedPageSizes).containsExactly(2, 2);

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"),
                                                        UTF8String.fromString("id > 0"),
                                                        2,
                                                        null
                                                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Only partition predicate is supported");
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void filteredListPagesAreSparseAndIterateByRawTokens() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Arrays.asList(spec("dt", "1"), spec("dt", "skip-a")), "p2"));
        catalog.addPage(
                "p2",
                new FormatTablePartitionPage(
                        Arrays.asList(spec("dt", "skip-b"), spec("dt", "skip-c")), "tail"));
        catalog.addPage(
                "tail",
                new FormatTablePartitionPage(Collections.singletonList(spec("dt", "3")), null));
        Predicate<Map<String, String>> numbered =
                partition -> Character.isDigit(partition.get("dt").charAt(0));

        List<Map<String, String>> matches = new ArrayList<>();
        List<Integer> pageRowCounts = new ArrayList<>();
        String token = null;
        do {
            FormatTablePartitionPage page =
                    ListFormatTablePartitionsProcedure.listPage(
                            catalog, Collections.emptyMap(), token, 2, numbered);
            pageRowCounts.add(page.partitions().size());
            matches.addAll(page.partitions());
            token = page.nextPageToken();
        } while (token != null);

        assertThat(matches).containsExactly(spec("dt", "1"), spec("dt", "3"));
        // The next-page token travels on result rows, so a raw page whose rows are all filtered
        // out is skipped inside the same call instead of surfacing an empty page that would lose
        // the cursor: the second call transparently advances from 'p2' to 'tail'.
        assertThat(pageRowCounts).containsExactly(1, 1);
        assertThat(catalog.requestedTokens).containsExactly(null, "p2", "tail");
        assertThat(catalog.requestedPageSizes).containsExactly(2, 2, 2);
    }

    @Test
    void listPageRejectsOversizedCatalogPage() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Arrays.asList(spec("dt", "1"), spec("dt", "2")), null));

        assertThatThrownBy(
                        () ->
                                ListFormatTablePartitionsProcedure.listPage(
                                        catalog, Collections.emptyMap(), null, 1, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned 2 partitions for a page of 1");
    }

    @Test
    void listPageForwardsPrefixStartingTokenAndLimit() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                "start",
                new FormatTablePartitionPage(
                        Collections.singletonList(spec("dt", "20260701")), "next"));

        FormatTablePartitionPage result =
                ListFormatTablePartitionsProcedure.listPage(
                        catalog, spec("dt", "202607"), "start", 1, null);

        assertThat(result.partitions()).containsExactly(spec("dt", "20260701"));
        assertThat(result.nextPageToken()).isEqualTo("next");
        assertThat(catalog.requestedPrefixes).containsExactly(spec("dt", "202607"));
        assertThat(catalog.requestedTokens).containsExactly("start");
        assertThat(catalog.requestedPageSizes).containsExactly(1);
    }

    @Test
    void listPageTreatsEmptyNextPageTokenAsTerminal() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Collections.singletonList(spec("dt", "20260701")), ""));

        FormatTablePartitionPage result =
                ListFormatTablePartitionsProcedure.listPage(
                        catalog, Collections.emptyMap(), null, 2, ignored -> true);

        assertThat(result.partitions()).containsExactly(spec("dt", "20260701"));
        assertThat(result.nextPageToken()).isNull();
        assertThat(catalog.requestedTokens).containsExactly((String) null);
    }

    @Test
    void syncDryRunReturnsFilesystemMinusCatalogWithoutCreatingPartitions() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Collections.singletonList(spec("dt", "20260701")), null));

        List<Map.Entry<Map<String, String>, String>> actions =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")),
                        Collections.singletonList("dt"),
                        true,
                        "ADD");

        assertThat(actions).containsExactly(action(spec("dt", "20260702"), "ADD"));
        assertThat(catalog.createdPartitions).isEmpty();
    }

    @Test
    void syncApplyCreatesTheWholeDiffAsAnIdempotentBatch() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(null, new FormatTablePartitionPage(Collections.emptyList(), null));

        List<Map.Entry<Map<String, String>, String>> actions =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")),
                        Collections.singletonList("dt"),
                        false,
                        "ADD");

        assertThat(actions)
                .containsExactly(
                        action(spec("dt", "20260701"), "ADD"),
                        action(spec("dt", "20260702"), "ADD"));
        assertThat(catalog.createdPartitions)
                .containsExactly(Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")));
        assertThat(catalog.createIgnoreFlags).containsExactly(true);
    }

    @Test
    void syncApplyPropagatesCreatePermissionFailureAndRefreshesCache() throws Exception {
        java.nio.file.Path partitionDirectory =
                Files.createDirectories(tempDir.resolve("dt=20260701"));
        Files.write(
                partitionDirectory.resolve("data.csv"),
                Collections.singletonList("1"),
                StandardCharsets.UTF_8);
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-permission-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            SecurityException failure = new SecurityException("partition create denied");
            RecordingCatalog partitionCatalog =
                    new RecordingCatalog() {
                        @Override
                        public void createPartitions(
                                List<Map<String, String>> partitions, boolean ignoreIfExists) {
                            throw failure;
                        }
                    };
            partitionCatalog.addPage(
                    null, new FormatTablePartitionPage(Collections.emptyList(), null));
            AtomicInteger refreshCount = new AtomicInteger();
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(
                            tableCatalog(
                                    new PaimonFormatTable(
                                            formatTable(tempDir.toUri().toString()),
                                            partitionCatalog))) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {
                            refreshCount.incrementAndGet();
                        }
                    };

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"), null, false
                                                    })))
                    .isSameAs(failure);
            assertThat(refreshCount).hasValue(1);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncApplyRefreshesAfterAddSucceedsAndDropFails() throws Exception {
        java.nio.file.Path partitionDirectory =
                Files.createDirectories(tempDir.resolve("dt=20260715"));
        Files.write(
                partitionDirectory.resolve("data.csv"),
                Collections.singletonList("15"),
                StandardCharsets.UTF_8);

        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-partial-success-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            IllegalStateException failure =
                    new IllegalStateException("injected partition drop failure");
            RecordingCatalog partitionCatalog =
                    new RecordingCatalog() {
                        @Override
                        public void dropPartitions(List<Map<String, String>> partitions) {
                            throw failure;
                        }
                    };
            partitionCatalog.addPage(
                    null,
                    new FormatTablePartitionPage(
                            Collections.singletonList(spec("dt", "20260714")), null));
            AtomicInteger refreshCount = new AtomicInteger();
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(
                            tableCatalog(
                                    new PaimonFormatTable(
                                            formatTable(tempDir.toUri().toString()),
                                            partitionCatalog))) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {
                            refreshCount.incrementAndGet();
                        }
                    };

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"),
                                                        UTF8String.fromString("SYNC"),
                                                        false
                                                    })))
                    .isSameAs(failure);
            assertThat(partitionCatalog.createdPartitions)
                    .containsExactly(Collections.singletonList(spec("dt", "20260715")));
            assertThat(refreshCount).hasValue(1);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncProcedureCallFailsClosedWhenSecondCatalogPageFails() throws Exception {
        java.nio.file.Path partitionDirectory =
                Files.createDirectories(tempDir.resolve("dt=20260715"));
        Files.write(
                partitionDirectory.resolve("data.csv"),
                Collections.singletonList("15"),
                StandardCharsets.UTF_8);

        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-second-page-failure-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            IllegalStateException listFailure =
                    new IllegalStateException("injected second catalog page failure");
            AtomicInteger listCount = new AtomicInteger();
            RecordingCatalog partitionCatalog =
                    new RecordingCatalog() {
                        @Override
                        public FormatTablePartitionPage listPartitions(
                                Map<String, String> prefix, String pageToken, int pageSize) {
                            listCount.incrementAndGet();
                            if ("page-2".equals(pageToken)) {
                                throw listFailure;
                            }
                            return super.listPartitions(prefix, pageToken, pageSize);
                        }
                    };
            partitionCatalog.addPage(
                    null,
                    new FormatTablePartitionPage(
                            Collections.singletonList(spec("dt", "20260714")), "page-2"));
            AtomicInteger refreshCount = new AtomicInteger();
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(
                            tableCatalog(
                                    new PaimonFormatTable(
                                            formatTable(tempDir.toUri().toString()),
                                            partitionCatalog))) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {
                            refreshCount.incrementAndGet();
                        }
                    };

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"),
                                                        UTF8String.fromString("SYNC"),
                                                        false
                                                    })))
                    .isSameAs(listFailure);
            assertThat(listCount).hasValue(2);
            assertThat(partitionCatalog.createdPartitions).isEmpty();
            assertThat(partitionCatalog.droppedPartitions).isEmpty();
            // Every non-preview attempt refreshes the Spark-side caches, even a failed one.
            assertThat(refreshCount).hasValue(1);
            assertThat(partitionDirectory).exists();
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncProcedureCallFailsClosedWhenFilesystemDiscoveryFailsPartway() throws Exception {
        Files.createDirectories(tempDir.resolve("dt=20260715/hh=00"));
        Files.createDirectories(tempDir.resolve("dt=20260716/hh=00"));

        IOException listFailure = new IOException("injected nested filesystem LIST failure");
        AtomicInteger completedPartitionListings = new AtomicInteger();
        FileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public FileStatus[] listStatus(Path path) throws IOException {
                        if ("dt=20260716".equals(path.getName())) {
                            throw listFailure;
                        }
                        FileStatus[] statuses = super.listStatus(path);
                        Arrays.sort(
                                statuses,
                                java.util.Comparator.comparing(
                                        status -> status.getPath().toString()));
                        if ("dt=20260715".equals(path.getName())) {
                            completedPartitionListings.incrementAndGet();
                        }
                        return statuses;
                    }
                };
        Map<String, String> options = new LinkedHashMap<>();
        options.put(CoreOptions.METASTORE_PARTITIONED_TABLE.key(), "true");
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("dt", DataTypes.STRING())
                        .field("hh", DataTypes.STRING())
                        .build();
        FormatTable partiallyListedTable =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("db", "t"))
                        .rowType(rowType)
                        .partitionKeys(Arrays.asList("dt", "hh"))
                        .location(new Path(tempDir.toUri().toString()).toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .build();

        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-filesystem-failure-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            AtomicInteger catalogListCount = new AtomicInteger();
            RecordingCatalog partitionCatalog =
                    new RecordingCatalog() {
                        @Override
                        public FormatTablePartitionPage listPartitions(
                                Map<String, String> prefix, String pageToken, int pageSize) {
                            catalogListCount.incrementAndGet();
                            return super.listPartitions(prefix, pageToken, pageSize);
                        }
                    };
            AtomicInteger refreshCount = new AtomicInteger();
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(
                            tableCatalog(
                                    new PaimonFormatTable(
                                            partiallyListedTable, partitionCatalog))) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {
                            refreshCount.incrementAndGet();
                        }
                    };

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"),
                                                        UTF8String.fromString("SYNC"),
                                                        false
                                                    })))
                    .isInstanceOf(RuntimeException.class)
                    .hasCause(listFailure);
            assertThat(completedPartitionListings).hasValue(1);
            assertThat(catalogListCount).hasValue(0);
            assertThat(partitionCatalog.createdPartitions).isEmpty();
            assertThat(partitionCatalog.droppedPartitions).isEmpty();
            // Every non-preview attempt refreshes the Spark-side caches, even a failed one.
            assertThat(refreshCount).hasValue(1);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncRejectsUnsafeValueOnlyDirectoryBeforeCatalogMutation() throws Exception {
        Files.createDirectories(tempDir.resolve("%2E%2E"));
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sync-format-table-metadata-value-only-safety-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            AtomicInteger catalogListCount = new AtomicInteger();
            RecordingCatalog partitionCatalog =
                    new RecordingCatalog() {
                        @Override
                        public FormatTablePartitionPage listPartitions(
                                Map<String, String> prefix, String pageToken, int pageSize) {
                            catalogListCount.incrementAndGet();
                            return super.listPartitions(prefix, pageToken, pageSize);
                        }
                    };
            Procedure procedure =
                    new SyncFormatTableMetadataProcedure(
                            tableCatalog(
                                    new PaimonFormatTable(
                                            formatTable(tempDir.toUri().toString(), true),
                                            partitionCatalog))) {
                        @Override
                        protected void refreshSparkCache(
                                org.apache.spark.sql.connector.catalog.Identifier ident,
                                org.apache.spark.sql.connector.catalog.Table table) {}
                    };

            assertThatThrownBy(
                            () ->
                                    procedure.call(
                                            new GenericInternalRow(
                                                    new Object[] {
                                                        UTF8String.fromString("db.t"),
                                                        UTF8String.fromString("SYNC"),
                                                        false
                                                    })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("..");
            assertThat(catalogListCount).hasValue(0);
            assertThat(partitionCatalog.createdPartitions).isEmpty();
            assertThat(partitionCatalog.droppedPartitions).isEmpty();
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void syncTreatsEmptyNextPageTokenAsTerminal() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Collections.singletonList(spec("dt", "20260701")), ""));

        List<Map.Entry<Map<String, String>, String>> actions =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Arrays.asList(spec("dt", "20260701"), spec("dt", "20260702")),
                        Collections.singletonList("dt"),
                        true,
                        "ADD");

        assertThat(actions).containsExactly(action(spec("dt", "20260702"), "ADD"));
        assertThat(catalog.requestedTokens).containsExactly((String) null);
    }

    @Test
    void syncWithNoDiffDoesNotIssueAnEmptyCreate() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Collections.singletonList(spec("dt", "20260701")), null));

        List<Map.Entry<Map<String, String>, String>> actions =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Collections.singletonList(spec("dt", "20260701")),
                        Collections.singletonList("dt"),
                        false,
                        "SYNC");

        assertThat(actions).isEmpty();
        assertThat(catalog.createdPartitions).isEmpty();
        assertThat(catalog.droppedPartitions).isEmpty();
    }

    @Test
    void syncRejectsUnsupportedMode() {
        assertThatThrownBy(() -> SyncFormatTableMetadataProcedure.validateMode("RECOVER"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ADD, DROP and SYNC");
        SyncFormatTableMetadataProcedure.validateMode("ADD");
        SyncFormatTableMetadataProcedure.validateMode("drop");
        SyncFormatTableMetadataProcedure.validateMode("Sync");
    }

    @Test
    void syncDropModeUnregistersOnlyMissingDirectories() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Arrays.asList(spec("dt", "20260714"), spec("dt", "20260715")), null));

        // dt=20260715 exists on the filesystem, dt=20260714 does not: only the latter is dropped.
        List<Map.Entry<Map<String, String>, String>> dryRun =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Collections.singletonList(spec("dt", "20260715")),
                        Collections.singletonList("dt"),
                        true,
                        "DROP");
        assertThat(dryRun).containsExactly(action(spec("dt", "20260714"), "DROP"));
        assertThat(catalog.droppedPartitions).isEmpty();

        List<Map.Entry<Map<String, String>, String>> applied =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Collections.singletonList(spec("dt", "20260715")),
                        Collections.singletonList("dt"),
                        false,
                        "DROP");
        assertThat(applied).containsExactly(action(spec("dt", "20260714"), "DROP"));
        assertThat(catalog.droppedPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260714")));
        assertThat(catalog.createdPartitions).isEmpty();
    }

    @Test
    void syncDropModeAppliesLargeDiffInBoundedBatches() {
        List<Map<String, String>> firstPage = new ArrayList<>();
        for (int index = 0; index < 1000; index++) {
            firstPage.add(spec("dt", String.format("%04d", index)));
        }

        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(null, new FormatTablePartitionPage(firstPage, "next"));
        catalog.addPage(
                "next",
                new FormatTablePartitionPage(Collections.singletonList(spec("dt", "1000")), null));

        List<Map.Entry<Map<String, String>, String>> applied =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        false,
                        "DROP");

        assertThat(applied).hasSize(1001);
        assertThat(catalog.droppedPartitions).hasSize(2);
        assertThat(catalog.droppedPartitions.get(0)).hasSize(1000);
        assertThat(catalog.droppedPartitions.get(1)).hasSize(1);
        assertThat(catalog.droppedPartitions.stream().mapToInt(List::size).sum()).isEqualTo(1001);
    }

    @Test
    void syncSyncModeAddsAndDropsInOneCall() {
        RecordingCatalog catalog = new RecordingCatalog();
        catalog.addPage(
                null,
                new FormatTablePartitionPage(
                        Collections.singletonList(spec("dt", "20260714")), null));

        List<Map.Entry<Map<String, String>, String>> applied =
                SyncFormatTableMetadataProcedure.syncPartitions(
                        catalog,
                        Collections.singletonList(spec("dt", "20260715")),
                        Collections.singletonList("dt"),
                        false,
                        "SYNC");
        assertThat(applied)
                .containsExactly(
                        action(spec("dt", "20260715"), "ADD"),
                        action(spec("dt", "20260714"), "DROP"));
        assertThat(catalog.createdPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260715")));
        assertThat(catalog.droppedPartitions)
                .containsExactly(Collections.singletonList(spec("dt", "20260714")));
    }

    private static Map.Entry<Map<String, String>, String> action(
            Map<String, String> spec, String action) {
        return new java.util.AbstractMap.SimpleEntry<>(spec, action);
    }

    private static Map<String, String> spec(String key, String value) {
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put(key, value);
        return spec;
    }

    private static FormatTable formatTable() {
        return formatTable("file:/tmp/format-table-procedure-test");
    }

    private static FormatTable formatTable(String location) {
        return formatTable(location, false);
    }

    private static FormatTable formatTable(String location, boolean onlyValueInPath) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(CoreOptions.METASTORE_PARTITIONED_TABLE.key(), "true");
        options.put(
                CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(),
                Boolean.toString(onlyValueInPath));
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("dt", DataTypes.STRING())
                        .build();
        return FormatTable.builder()
                .fileIO(LocalFileIO.create())
                .identifier(Identifier.create("db", "t"))
                .rowType(rowType)
                .partitionKeys(Collections.singletonList("dt"))
                .location(new Path(location).toString())
                .format(FormatTable.Format.CSV)
                .options(options)
                .build();
    }

    private static TableCatalog tableCatalog(PaimonFormatTable table) {
        return (TableCatalog)
                Proxy.newProxyInstance(
                        FormatTableMetadataProcedureTest.class.getClassLoader(),
                        new Class<?>[] {TableCatalog.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "name":
                                    return "test_catalog";
                                case "defaultNamespace":
                                    return new String[] {"db"};
                                case "loadTable":
                                    return table;
                                case "equals":
                                    return proxy == args[0];
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "toString":
                                    return "test_catalog";
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }

    private static class RecordingCatalog implements FormatTablePartitionCatalog {

        private final Map<String, FormatTablePartitionPage> pages = new LinkedHashMap<>();
        private final List<String> requestedTokens = new ArrayList<>();
        private final List<Integer> requestedPageSizes = new ArrayList<>();
        private final List<Map<String, String>> requestedPrefixes = new ArrayList<>();
        private final List<List<Map<String, String>>> createdPartitions = new ArrayList<>();
        private final List<Boolean> createIgnoreFlags = new ArrayList<>();
        private final List<List<Map<String, String>>> droppedPartitions = new ArrayList<>();

        private void addPage(String token, FormatTablePartitionPage page) {
            pages.put(token, page);
        }

        @Override
        public void createPartitions(List<Map<String, String>> partitions, boolean ignoreIfExists) {
            createdPartitions.add(new ArrayList<>(partitions));
            createIgnoreFlags.add(ignoreIfExists);
        }

        @Override
        public void dropPartitions(List<Map<String, String>> partitions) {
            droppedPartitions.add(new ArrayList<>(partitions));
        }

        @Override
        public List<Map<String, String>> listPartitionsByNames(
                List<Map<String, String>> partitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FormatTablePartitionPage listPartitions(
                Map<String, String> prefix, String pageToken, int pageSize) {
            requestedPrefixes.add(prefix == null ? null : new LinkedHashMap<>(prefix));
            requestedTokens.add(pageToken);
            requestedPageSizes.add(pageSize);
            return pages.get(pageToken);
        }
    }
}

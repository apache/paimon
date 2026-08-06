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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.CompactManifestAction;
import org.apache.paimon.table.FileStoreTable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

/** IT Case for {@link CompactManifestProcedure}. */
public class CompactManifestProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testManifestCompactProcedure() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'file.format' = 'parquet',"
                        + " 'manifest.full-compaction-threshold-size' = '10000 T',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql(
                "INSERT INTO T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T VALUES (1, '101', 15, '20221208'), (4, '1001', 16, '20221208'), (5, '10001', 15, '20221209')");

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T$manifests").get(0).getField(0))
                .isEqualTo(9L);

        Assertions.assertThat(
                        Objects.requireNonNull(
                                        sql("CALL sys.compact_manifest(`table` => 'default.T')")
                                                .get(0)
                                                .getField(0))
                                .toString())
                .isEqualTo("success");

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T$manifests").get(0).getField(0))
                .isEqualTo(0L);

        Assertions.assertThat(sql("SELECT * FROM T ORDER BY k").toString())
                .isEqualTo(
                        "[+I[1, 101, 15, 20221208], +I[4, 1001, 16, 20221208], +I[5, 10001, 15, 20221209]]");
    }

    @Test
    public void testManifestSortParameters() throws Exception {
        sql(
                "CREATE TABLE T_SORT ("
                        + " k INT,"
                        + " v STRING,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'manifest.full-compaction-threshold-size' = '10000 T',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql("INSERT INTO T_SORT VALUES (1, '10', '20221208'), (2, '20', '20221209')");
        sql("INSERT OVERWRITE T_SORT VALUES (1, '11', '20221208'), (2, '21', '20221209')");

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T_SORT$manifests")
                                .get(0)
                                .getField(0))
                .isEqualTo(2L);

        String procedure =
                "CALL sys.compact_manifest("
                        + "`table` => 'default.T_SORT', "
                        + "`options` => 'manifest-sort.partition-field=missing', "
                        + "`manifest_sort_enabled` => true, "
                        + "`manifest_sort_partition_field` => 'dt', "
                        + "`manifest_sort_max_rewrite_size` => '1 gb')";
        sql(procedure);

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T_SORT$manifests")
                                .get(0)
                                .getField(0))
                .isEqualTo(0L);

        FileStoreTable table = paimonTable("T_SORT");
        long compactSnapshotId = table.snapshotManager().latestSnapshot().id();
        sql(procedure);
        Assertions.assertThat(table.snapshotManager().latestSnapshot().id())
                .isEqualTo(compactSnapshotId);
    }

    @Test
    public void testManifestSortParametersValidation() {
        sql(
                "CREATE TABLE T_INVALID (k INT, dt STRING) PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '-1'"
                        + ")");

        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.compact_manifest("
                                                + "`table` => 'default.T_INVALID', "
                                                + "`manifest_sort_enabled` => true, "
                                                + "`manifest_sort_partition_field` => 'missing')"))
                .hasStackTraceContaining(
                        "'manifest-sort.partition-field' = 'missing' is not a partition field");
    }

    @Test
    public void testManifestCompactWithoutSnapshotDoesNotCommit() throws Exception {
        sql(
                "CREATE TABLE T_EMPTY (k INT, dt STRING) PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '-1'"
                        + ")");
        FileStoreTable table = paimonTable("T_EMPTY");
        Assertions.assertThat(table.snapshotManager().latestSnapshot()).isNull();

        sql(
                "CALL sys.compact_manifest("
                        + "`table` => 'default.T_EMPTY', "
                        + "`manifest_sort_enabled` => true, "
                        + "`manifest_sort_partition_field` => 'dt')");

        Assertions.assertThat(table.snapshotManager().latestSnapshot()).isNull();
    }

    @Test
    public void testManifestCompactActionWithManifestSort() throws Exception {
        sql(
                "CREATE TABLE T_ACTION ("
                        + " k INT,"
                        + " v STRING,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'manifest.full-compaction-threshold-size' = '10000 T',"
                        + " 'bucket' = '-1'"
                        + ")");
        sql("INSERT INTO T_ACTION VALUES (1, '10', '20221208'), (2, '20', '20221209')");
        sql("INSERT OVERWRITE T_ACTION VALUES (1, '11', '20221208'), (2, '21', '20221209')");

        CompactManifestAction action =
                ActionFactory.createAction(
                                new String[] {
                                    "compact_manifest",
                                    "--warehouse",
                                    path,
                                    "--database",
                                    "default",
                                    "--table",
                                    "T_ACTION",
                                    "--manifest-sort.enabled",
                                    "true",
                                    "--manifest-sort.partition-field",
                                    "dt",
                                    "--manifest-sort.max-rewrite-size",
                                    "1gb"
                                })
                        .filter(CompactManifestAction.class::isInstance)
                        .map(CompactManifestAction.class::cast)
                        .orElseThrow(() -> new RuntimeException("Failed to create action"));
        action.run();

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T_ACTION$manifests")
                                .get(0)
                                .getField(0))
                .isEqualTo(0L);
    }

    @Test
    public void testManifestCompactProcedureWithBranch() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'manifest.full-compaction-threshold-size' = '10000 T',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql(
                "INSERT INTO `T` VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql("CALL sys.create_tag('default.T', 'tag1', 1)");

        sql("call sys.create_branch('default.T', 'branch1', 'tag1')");

        sql(
                "INSERT OVERWRITE T$branch_branch1 VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T$branch_branch1 VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T$branch_branch1 VALUES (1, '101', 15, '20221208'), (4, '1001', 16, '20221208'), (5, '10001', 15, '20221209')");

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T$branch_branch1$manifests")
                                .get(0)
                                .getField(0))
                .isEqualTo(9L);

        Assertions.assertThat(
                        Objects.requireNonNull(
                                        sql("CALL sys.compact_manifest(`table` => 'default.T$branch_branch1')")
                                                .get(0)
                                                .getField(0))
                                .toString())
                .isEqualTo("success");

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T$branch_branch1$manifests")
                                .get(0)
                                .getField(0))
                .isEqualTo(0L);

        Assertions.assertThat(sql("SELECT * FROM T$branch_branch1 ORDER BY k").toString())
                .isEqualTo(
                        "[+I[1, 101, 15, 20221208], +I[4, 1001, 16, 20221208], +I[5, 10001, 15, 20221209]]");
    }

    @Test
    public void testManifestCompactDryRun() {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'manifest.full-compaction-threshold-size' = '10000 T',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql(
                "INSERT INTO T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT OVERWRITE T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T$manifests").get(0).getField(0))
                .isEqualTo(6L);

        String dryRunResult =
                Objects.requireNonNull(
                                sql("CALL sys.compact_manifest(`table` => 'default.T', `dry_run` => true)")
                                        .get(0)
                                        .getField(0))
                        .toString();

        Assertions.assertThat(dryRunResult).startsWith("Dry run:");
        Assertions.assertThat(dryRunResult).contains("deleted entries in");

        // verify dry run did not actually compact
        Assertions.assertThat(
                        sql("SELECT sum(num_deleted_files) FROM T$manifests").get(0).getField(0))
                .isEqualTo(6L);
    }
}

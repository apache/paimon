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
}

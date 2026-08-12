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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions

import scala.jdk.CollectionConverters._

/** Test rewrite manifest procedure. See [[RewriteManifestProcedure]]. */
class RewriteManifestProcedureTest extends PaimonSparkTestBase with StreamTest {

  test("Paimon Procedure: rewrite manifest cleans delete entries and keeps data intact") {
    // A small manifest target file size forces several manifest files, so that the global sort
    // actually reshuffles entries across files.
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '2',
                 |  'bucket-key' = 'id',
                 |  'write-only' = 'true',
                 |  'file.format' = 'avro',
                 |  'manifest.target-file-size' = '1KB'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    // Insert partitions in non-sorted order across multiple commits so that manifest entries are
    // interleaved and out of partition order.
    spark.sql(s"INSERT INTO T VALUES (1, 'a', '2024-01-03')")
    spark.sql(s"INSERT INTO T VALUES (2, 'b', '2024-01-01')")
    spark.sql(s"INSERT INTO T VALUES (3, 'c', '2024-01-02')")
    spark.sql(s"INSERT INTO T VALUES (4, 'd', '2024-01-01')")
    spark.sql(s"INSERT INTO T VALUES (5, 'e', '2024-01-03')")

    // Partition-level overwrites produce ADD/DELETE pairs that rewrite_manifest should cancel,
    // without dropping the other partitions.
    spark.sql(s"INSERT OVERWRITE T PARTITION (dt = '2024-01-03') VALUES (1, 'a2')")
    spark.sql(s"INSERT OVERWRITE T PARTITION (dt = '2024-01-01') VALUES (2, 'b2'), (4, 'd2')")

    Thread.sleep(10000)

    val expectedCount = spark.sql("SELECT count(*) FROM T").collectAsList().get(0).getLong(0)
    val expectedIdSum = spark.sql("SELECT sum(id) FROM T").collectAsList().get(0).getLong(0)

    // before rewrite there should be some delete entries
    val beforeDeleted =
      spark
        .sql("SELECT sum(num_deleted_files) FROM `T$manifests`")
        .collectAsList()
        .get(0)
        .getLong(0)
    Assertions.assertThat(beforeDeleted).isGreaterThan(0L)

    val beforeManifests =
      spark.sql("SELECT count(*) FROM `T$manifests`").collectAsList().get(0).getLong(0)

    val result =
      spark.sql("CALL sys.rewrite_manifest(table => 'T')").collectAsList().get(0)

    // rewritten_manifests_count > 0 and added_manifests_count == after - before
    val rewrittenCount = result.getInt(0)
    val addedCount = result.getInt(1)
    Assertions.assertThat(rewrittenCount).isGreaterThan(0)
    Assertions.assertThat(addedCount).isEqualTo(rewrittenCount - beforeManifests.toInt)

    // after rewrite all delete entries must be cleaned
    val afterDeleted =
      spark
        .sql("SELECT sum(num_deleted_files) FROM `T$manifests`")
        .collectAsList()
        .get(0)
        .getLong(0)
    Assertions.assertThat(afterDeleted).isEqualTo(0L)

    // data must be intact
    Assertions
      .assertThat(spark.sql("SELECT count(*) FROM T").collectAsList().get(0).getLong(0))
      .isEqualTo(expectedCount)
    Assertions
      .assertThat(spark.sql("SELECT sum(id) FROM T").collectAsList().get(0).getLong(0))
      .isEqualTo(expectedIdSum)
  }

  test("Paimon Procedure: rewrite manifest orders manifest partitions globally") {
    spark.sql(s"""
                 |CREATE TABLE T2 (id INT, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'write-only' = 'true',
                 |  'manifest.target-file-size' = '1KB',
                 |  'file.format' = 'avro'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    // insert partitions in reverse order across commits
    spark.sql(s"INSERT INTO T2 VALUES (1, '2024-01-03')")
    spark.sql(s"INSERT INTO T2 VALUES (2, '2024-01-02')")
    spark.sql(s"INSERT INTO T2 VALUES (3, '2024-01-01')")
    spark.sql(s"INSERT INTO T2 VALUES (4, '2024-01-03')")
    spark.sql(s"INSERT INTO T2 VALUES (5, '2024-01-01')")

    Thread.sleep(10000)

    val expectedCount = spark.sql("SELECT count(*) FROM T2").collectAsList().get(0).getLong(0)

    spark.sql("CALL sys.rewrite_manifest(table => 'T2')")

    // After global sort, manifest partition ranges must not overlap: sorting manifests by their
    // min partition must yield a sequence where each manifest's min is >= the previous manifest's
    // max. This is robust regardless of the row order Spark returns for the system table.
    val ranges = spark
      .sql("SELECT min_partition_stats, max_partition_stats FROM `T2$manifests`")
      .collectAsList()
      .asScala
      .filter(r => !r.isNullAt(0))
      .map(r => (r.getString(0), r.getString(1)))
      .sortBy(_._1)

    for (i <- 1 until ranges.length) {
      Assertions.assertThat(ranges(i)._1.compareTo(ranges(i - 1)._2) >= 0).isTrue
    }

    // data intact
    Assertions
      .assertThat(spark.sql("SELECT count(*) FROM T2").collectAsList().get(0).getLong(0))
      .isEqualTo(expectedCount)
  }

  test("Paimon Procedure: rewrite manifest with where only rewrites matching manifests") {
    spark.sql(s"""
                 |CREATE TABLE T3 (id INT, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'write-only' = 'true',
                 |  'manifest.target-file-size' = '1KB'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    // insert into multiple partitions across commits
    spark.sql(s"INSERT INTO T3 VALUES (1, '2024-01-01')")
    spark.sql(s"INSERT INTO T3 VALUES (2, '2024-01-02')")
    spark.sql(s"INSERT INTO T3 VALUES (3, '2024-01-03')")
    spark.sql(s"INSERT INTO T3 VALUES (4, '2024-01-01')")

    Thread.sleep(10000)

    val expectedCount = spark.sql("SELECT count(*) FROM T3").collectAsList().get(0).getLong(0)

    // record manifest file names before rewrite
    val allManifestsBefore =
      spark.sql("SELECT file_name FROM `T3$manifests`").collectAsList().asScala.map(_.getString(0))

    // rewrite only manifests that may match dt = '2024-01-01'
    spark.sql("CALL sys.rewrite_manifest(table => 'T3', where => 'dt = \"2024-01-01\"')")

    val allManifestsAfter =
      spark.sql("SELECT file_name FROM `T3$manifests`").collectAsList().asScala.map(_.getString(0))

    // some manifests should have been rewritten (new file names appear)
    val newManifests = allManifestsAfter.filter(!allManifestsBefore.contains(_))
    Assertions.assertThat(newManifests.nonEmpty).isTrue

    // but not all manifests are rewritten — at least one original manifest survives
    val survivingOriginals = allManifestsBefore.filter(allManifestsAfter.contains(_))
    Assertions.assertThat(survivingOriginals.nonEmpty).isTrue

    // data intact
    Assertions
      .assertThat(spark.sql("SELECT count(*) FROM T3").collectAsList().get(0).getLong(0))
      .isEqualTo(expectedCount)
    Assertions
      .assertThat(
        spark
          .sql("SELECT count(*) FROM T3 WHERE dt = '2024-01-01'")
          .collectAsList()
          .get(0)
          .getLong(0))
      .isEqualTo(2)
  }

  test("Paimon Procedure: rewrite manifest with where matching nothing returns zero") {
    spark.sql(s"""
                 |CREATE TABLE T4 (id INT, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'write-only' = 'true',
                 |  'manifest.target-file-size' = '1KB',
                 |  'file.format' = 'avro'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T4 VALUES (1, '2024-01-01')")
    spark.sql(s"INSERT INTO T4 VALUES (2, '2024-01-02')")
    Thread.sleep(10000)

    val manifestsBefore =
      spark.sql("SELECT file_name FROM `T4$manifests`").collectAsList().asScala.map(_.getString(0))

    // where matches no partition — nothing should be rewritten
    val result =
      spark
        .sql("CALL sys.rewrite_manifest(table => 'T4', where => 'dt = \"1999-01-01\"')")
        .collectAsList()
        .get(0)
    Assertions.assertThat(result.getInt(0)).isEqualTo(0)
    Assertions.assertThat(result.getInt(1)).isEqualTo(0)

    // all original manifests survive untouched
    val manifestsAfter =
      spark.sql("SELECT file_name FROM `T4$manifests`").collectAsList().asScala.map(_.getString(0))
    Assertions.assertThat(manifestsAfter).isEqualTo(manifestsBefore)

    // data intact
    Assertions
      .assertThat(spark.sql("SELECT count(*) FROM T4").collectAsList().get(0).getLong(0))
      .isEqualTo(2)
  }

  test("Paimon Procedure: rewrite manifest with range where rewrites matching partitions") {
    spark.sql(s"""
                 |CREATE TABLE T5 (id INT, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'write-only' = 'true',
                 |  'manifest.target-file-size' = '1KB',
                 |  'file.format' = 'avro'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T5 VALUES (1, '2024-01-01')")
    spark.sql(s"INSERT INTO T5 VALUES (2, '2024-01-02')")
    spark.sql(s"INSERT INTO T5 VALUES (3, '2024-01-03')")
    spark.sql(s"INSERT INTO T5 VALUES (4, '2024-01-04')")
    Thread.sleep(10000)

    val expectedCount = spark.sql("SELECT count(*) FROM T5").collectAsList().get(0).getLong(0)

    // range where: only Jan 1-2
    spark.sql(
      "CALL sys.rewrite_manifest(table => 'T5', where => 'dt >= \"2024-01-01\" AND dt <= \"2024-01-02\"')")

    // data intact across all partitions
    Assertions
      .assertThat(spark.sql("SELECT count(*) FROM T5").collectAsList().get(0).getLong(0))
      .isEqualTo(expectedCount)
    Assertions
      .assertThat(
        spark
          .sql("SELECT count(*) FROM T5 WHERE dt >= '2024-01-01' AND dt <= '2024-01-02'")
          .collectAsList()
          .get(0)
          .getLong(0))
      .isEqualTo(2)
  }

  test("Paimon Procedure: rewrite manifest on unpartitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T6 (id INT, value STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'write-only' = 'true',
                 |  'manifest.target-file-size' = '1KB',
                 |  'file.format' = 'avro'
                 |)
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T6 VALUES (1, 'a')")
    spark.sql(s"INSERT INTO T6 VALUES (2, 'b')")
    Thread.sleep(10000)

    val expectedCount = spark.sql("SELECT count(*) FROM T6").collectAsList().get(0).getLong(0)

    spark.sql("CALL sys.rewrite_manifest(table => 'T6')")

    // data intact
    Assertions
      .assertThat(spark.sql("SELECT count(*) FROM T6").collectAsList().get(0).getLong(0))
      .isEqualTo(expectedCount)
  }

  test("Paimon Procedure: rewrite manifest with non-partition where throws") {
    spark.sql(s"""
                 |CREATE TABLE T7 (id INT, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'write-only' = 'true',
                 |  'manifest.target-file-size' = '1KB',
                 |  'file.format' = 'avro'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T7 VALUES (1, '2024-01-01')")
    Thread.sleep(10000)

    // where on a non-partition column (id) must fail
    Assertions
      .assertThatThrownBy(
        () => spark.sql("CALL sys.rewrite_manifest(table => 'T7', where => 'id = 1')"))
      .hasMessageContaining("Only partition predicate is supported")
  }

  test("Paimon Procedure: rewritten manifest sizes are within the target bound") {
    // Use a small target file size and enough data so that multiple manifests are produced.
    // Each task writes a single non-rolling manifest, so every output manifest should be at most
    // a modest multiple of the target size (allowing headroom for a single partition that
    // slightly exceeds the range estimate).
    val targetSize = 1024L
    spark.sql(s"""
                 |CREATE TABLE T8 (id INT, value STRING, dt STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '4',
                 |  'bucket-key' = 'id',
                 |  'write-only' = 'true',
                 |  'file.format' = 'avro',
                 |  'manifest.target-file-size' = '${targetSize}B'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    // Insert enough rows across enough partitions to produce multiple output manifests.
    for (dt <- 0 until 20) {
      val values = (0 until 20).map(i => s"(${dt * 20 + i}, '${"x" * 50}', '2024-01-${dt + 1}')")
      spark.sql(s"INSERT INTO T8 VALUES ${values.mkString(", ")}")
    }
    Thread.sleep(10000)

    spark.sql("CALL sys.rewrite_manifest(table => 'T8')")

    val sizes =
      spark.sql("SELECT file_size FROM `T8$manifests`").collectAsList().asScala.map(_.getLong(0))
    Assertions.assertThat(sizes.nonEmpty).isTrue
    // Every rewritten manifest should be <= 3x target size (generous bound for the last manifest
    // of a task and range-partitioner skew). The key invariant: no manifest should be wildly
    // oversized (e.g. 10x), which would indicate the parallelism estimate is wrong.
    for (size <- sizes) {
      Assertions.assertThat(size).isLessThanOrEqualTo(targetSize * 3)
    }
  }
}

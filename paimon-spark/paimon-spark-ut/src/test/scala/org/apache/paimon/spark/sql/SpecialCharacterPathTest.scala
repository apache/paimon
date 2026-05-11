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

package org.apache.paimon.spark.sql

import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.DateTimeUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import java.util.concurrent.TimeUnit

class SpecialCharacterPathTest extends PaimonSparkTestBase {

  val warehouseDir: String = tempDBDir.getCanonicalPath + "/22%3A45%3A30"

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.warehouse", warehouseDir)
  }

  test("delete with timestamp partition column") {
    for (useV2Write <- Seq("true", "false")) {
      for (dvEnabled <- Seq("true", "false")) {
        withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
          withTable("t") {
            sql(s"""
                   |CREATE TABLE t (id INT, dt TIMESTAMP)
                   |PARTITIONED BY (dt)
                   |TBLPROPERTIES (
                   | 'deletion-vectors.enabled' = '$dvEnabled'
                   |)
                   |""".stripMargin)
            val table = loadTable("t")
            val fileIO = table.fileIO()

            sql(
              "INSERT INTO t SELECT id, CAST('2024-06-02 15:45:30' AS TIMESTAMP) FROM RANGE(0, 1000)")
            checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
            checkAnswer(sql("SELECT sum(id) FROM t"), Seq(Row(499500)))

            val fileDir = new Path(warehouseDir, "test.db/t/dt=2024-06-02T22%3A45%3A30/bucket-0")
            assert(fileIO.exists(fileDir))
            val filePath =
              sql("SELECT __paimon_file_path FROM t LIMIT 1").collect().head.getString(0)
            assert(fileIO.exists(new Path(filePath)))

            sql("DELETE FROM t WHERE id = 100")
            checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(999)))
            checkAnswer(sql("SELECT sum(id) FROM t"), Seq(Row(499400)))
          }
        }
      }
    }
  }

  test("remove orphan files") {
    withTable("t") {
      val orphanFile1Name = "bucket-0/orphan_file1"
      val orphanFile2Name = "bucket-0/orphan_file2"

      sql("CREATE TABLE t (id STRING, name STRING)")

      sql(s"INSERT INTO t VALUES ('1', 'a'), ('2', 'b')")

      val table = loadTable("t")
      val fileIO = table.fileIO()
      val tablePath = table.location()

      val orphanFile1 = new Path(tablePath, orphanFile1Name)
      val orphanFile2 = new Path(tablePath, orphanFile2Name)

      fileIO.tryToWriteAtomic(orphanFile1, "a")
      Thread.sleep(2000)
      fileIO.tryToWriteAtomic(orphanFile2, "b")

      val orphanFileModTime = fileIO.getFileStatus(orphanFile2).getModificationTime
      val older_than = DateTimeUtils.formatLocalDateTime(
        DateTimeUtils.toLocalDateTime(
          orphanFileModTime -
            TimeUnit.SECONDS.toMillis(1)),
        3)
      checkAnswer(
        sql(s"CALL sys.remove_orphan_files(table => 't', older_than => '$older_than')"),
        Row(1, 1) :: Nil)
    }
  }

  test("data file external paths") {
    withTempDir {
      tmpDir =>
        withTable("t") {
          val externalPath = s"file://${tmpDir.getCanonicalPath}/22%3A45%3A3123456"
          sql(s"""
                 |CREATE TABLE t (id INT, v INT, dt TIMESTAMP)
                 |TBLPROPERTIES (
                 | 'bucket-key' = 'id',
                 | 'bucket' = '1',
                 | 'data-file.external-paths' = '$externalPath',
                 | 'data-file.external-paths.strategy' = 'round-robin'
                 |)
                 |PARTITIONED BY (dt)
                 |""".stripMargin)
          sql(
            "INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id, CAST('2024-06-02 15:45:30' AS TIMESTAMP) FROM range (1, 50000)")

          val filePath =
            sql("SELECT __paimon_file_path FROM t WHERE __paimon_file_path LIKE '%123456%' LIMIT 1")
              .collect()
              .head
              .getString(0)
          assert(fileIO.exists(new Path(filePath)))
          assert(fileIO.exists(new Path(externalPath)))

          sql("DELETE FROM t WHERE id >= 111 and id <= 444")
          checkAnswer(sql("SELECT count(*) FROM t"), Row(49665))
          checkAnswer(sql("SELECT sum(v) FROM t"), Row(1249882315L))

          sql("UPDATE t SET v = v + 1 WHERE id >= 555 and id <= 666")
          checkAnswer(sql("SELECT count(*) FROM t"), Row(49665))
          checkAnswer(sql("SELECT sum(v) FROM t"), Row(1249882427L))

          sql("UPDATE t SET v = v + 1 WHERE id >= 600 and id <= 800")
          checkAnswer(sql("SELECT count(*) FROM t"), Row(49665))
          checkAnswer(sql("SELECT sum(v) FROM t"), Row(1249882628L))
        }
    }
  }
}

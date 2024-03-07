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

import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.DateTimeUtils

import org.apache.spark.sql.Row

import java.util.concurrent.TimeUnit

class RemoveOrphanFilesProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: remove orphan files") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a'), ('2', 'b')")

    val table = loadTable("T")
    val fileIO = table.fileIO()
    val tablePath = table.location()

    val orphanFile1 = new Path(tablePath, "bucket-0/orphan_file1")
    val orphanFile2 = new Path(tablePath, "bucket-0/orphan_file2")

    fileIO.writeFileUtf8(orphanFile1, "a")
    Thread.sleep(2000)
    fileIO.writeFileUtf8(orphanFile2, "b")

    // by default, no file deleted
    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row("Deleted=0") :: Nil)

    val orphanFile2ModTime = fileIO.getFileStatus(orphanFile2).getModificationTime
    val older_than1 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(
        orphanFile2ModTime -
          TimeUnit.SECONDS.toMillis(1)),
      3)

    checkAnswer(
      spark.sql(s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than1')"),
      Row("Deleted=1") :: Nil)

    val older_than2 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)

    checkAnswer(
      spark.sql(s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than2')"),
      Row("Deleted=1") :: Nil)
  }
}

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

import org.apache.paimon.CoreOptions
import org.apache.paimon.fs.{FileStatus, Path}
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions

class LookupCompactionTest extends PaimonSparkTestBase {

  test("Paimon lookup compaction: number of data file written") {
    CoreOptions.MergeEngine.values().foreach {
      mergeEngine =>
        withTable("T") {
          val extraOptions = if (mergeEngine == CoreOptions.MergeEngine.AGGREGATE) {
            s", 'fields.count.aggregate-function' = 'sum'"
          } else {
            ""
          }

          spark.sql(
            s"""
               |CREATE TABLE T (id INT, name STRING, count INT)
               |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'merge-engine' = '$mergeEngine', 'changelog-producer' = 'lookup' $extraOptions)
               |""".stripMargin)

          val table = loadTable("T")
          val tabLocation = table.tableDataPath()
          val fileIO = table.fileIO()

          // First insert, file is upgraded to the max level when compaction, no need rewrite
          spark.sql("INSERT INTO T VALUES (1, 'aaaaaaaaaaa', 1), (2, 'b', 2)")
          var files = fileIO.listStatus(new Path(tabLocation, "bucket-0"))
          Assertions.assertEquals(1, dataFileCount(files))

          checkAnswer(
            spark.sql("SELECT * FROM T ORDER BY id"),
            Row(1, "aaaaaaaaaaa", 1) :: Row(2, "b", 2) :: Nil)

          spark.sql("INSERT INTO T VALUES (2, 'b', 22), (3, 'c', 3)")
          files = fileIO.listStatus(new Path(tabLocation, "bucket-0"))
          // Second insert, file is upgraded to other lower level when compaction, only DEDUPLICATE can skip rewrite
          if (mergeEngine == CoreOptions.MergeEngine.DEDUPLICATE) {
            Assertions.assertEquals(2, dataFileCount(files))
          } else {
            Assertions.assertEquals(3, dataFileCount(files))
          }

          val df = spark.sql("SELECT * FROM T ORDER BY id")
          mergeEngine match {
            case CoreOptions.MergeEngine.DEDUPLICATE | CoreOptions.MergeEngine.PARTIAL_UPDATE =>
              checkAnswer(df, Row(1, "aaaaaaaaaaa", 1) :: Row(2, "b", 22) :: Row(3, "c", 3) :: Nil)
            case CoreOptions.MergeEngine.AGGREGATE =>
              checkAnswer(df, Row(1, "aaaaaaaaaaa", 1) :: Row(2, "b", 24) :: Row(3, "c", 3) :: Nil)
            case CoreOptions.MergeEngine.FIRST_ROW =>
              checkAnswer(df, Row(1, "aaaaaaaaaaa", 1) :: Row(2, "b", 2) :: Row(3, "c", 3) :: Nil)
            case _ =>
          }
        }
    }
  }

  private def dataFileCount(files: Array[FileStatus]): Int = {
    files.count(f => f.getPath.getName.startsWith("data"))
  }
}

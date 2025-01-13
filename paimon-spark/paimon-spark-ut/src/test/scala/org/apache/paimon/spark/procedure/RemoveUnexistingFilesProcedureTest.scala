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

import org.apache.paimon.operation.ListUnexistingFilesTest
import org.apache.paimon.spark.PaimonSparkTestBase

import java.util.UUID

class RemoveUnexistingFilesProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: remove unexisting files, bucket = -1") {
    testImpl(-1)
  }

  test("Paimon procedure: remove unexisting files, bucket = 3") {
    testImpl(3)
  }

  private def testImpl(bucket: Int): Unit = {
    val warehouse = tempDBDir.getCanonicalPath

    val numPartitions = 2
    val numFiles = 10
    val numDeletes = new Array[Int](numPartitions)
    val tableName = "t_" + UUID.randomUUID().toString.replace("-", "_")
    ListUnexistingFilesTest.prepareRandomlyDeletedTable(
      warehouse,
      "mydb",
      tableName,
      bucket,
      numFiles,
      numDeletes)

    val actual = new Array[Int](numPartitions)
    val pattern = "pt=(\\d+?)/".r
    spark.sql(s"USE mydb")
    spark
      .sql(s"CALL sys.remove_unexisting_files(table => '$tableName', dry_run => true)")
      .collect()
      .foreach(
        r => {
          pattern.findFirstMatchIn(r.getString(0)) match {
            case Some(m) => actual(m.group(1).toInt) += 1
          }
        })
    assert(actual.toSeq == numDeletes.toSeq)

    spark.sql(s"CALL sys.remove_unexisting_files(table => '$tableName')")
    spark
      .sql(s"SELECT pt, CAST(COUNT(*) AS INT) FROM $tableName GROUP BY pt")
      .collect()
      .foreach(
        r => {
          assert(r.getInt(1) == numFiles - numDeletes(r.getInt(0)))
        })
  }
}

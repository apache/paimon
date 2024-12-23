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
import org.apache.paimon.partition.file.SuccessFile
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

class MarkPartitionDoneProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: mark_partition_done test") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, day STRING)
                 |USING PAIMON
                 |PARTITIONED BY (day)
                 |TBLPROPERTIES (
                 |'primary-key'='day,id',
                 |'partition.mark-done-action'='success-file')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', '2024-07-13')")
    spark.sql(s"INSERT INTO T VALUES ('2', 'b', '2024-07-14')")

    checkAnswer(
      spark.sql(
        "CALL paimon.sys.mark_partition_done(" +
          "table => 'test.T', partitions => 'day=2024-07-13;day=2024-07-14')"),
      Row(true) :: Nil)

    val table = loadTable("T")

    val successPath1 = new Path(table.tableDataPath, "day=2024-07-13/_SUCCESS")
    val successFile1 = SuccessFile.safelyFromPath(table.fileIO, successPath1)
    assertThat(successFile1).isNotNull

    val successPath2 = new Path(table.tableDataPath, "day=2024-07-14/_SUCCESS")
    val successFile2 = SuccessFile.safelyFromPath(table.fileIO, successPath2)
    assertThat(successFile2).isNotNull

  }

}

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

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions

class ReplaceTableWithRestCatalogTest extends PaimonSparkTestWithRestCatalogBase {

  import testImplicits._

  test("Replace table with REST catalog: REPLACE TABLE preserves snapshots") {
    assume(gteqSpark3_4)
    withTable("t") {
      sql("""
            |CREATE TABLE t (id BIGINT, data STRING)
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'old')")
      val oldLocation = loadTable("t").location().toString
      val oldSnapshotId = loadTable("t").snapshotManager().latestSnapshotId()

      sql("""
            |REPLACE TABLE t (id BIGINT, name STRING)
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '4')
            |""".stripMargin)

      val replaced = loadTable("t")
      Assertions.assertEquals(oldLocation, replaced.location().toString)
      Assertions.assertEquals("4", replaced.options().get("bucket"))
      Assertions.assertEquals(Seq("id", "name"), spark.table("t").schema.fieldNames.toSeq)
      checkAnswer(sql("SELECT * FROM t"), Seq.empty[Row])

      checkAnswer(
        sql(s"SELECT id, data FROM t VERSION AS OF $oldSnapshotId"),
        Seq((1L, "old")).toDF())
    }
  }

  test("Replace table with REST catalog: CREATE OR REPLACE TABLE AS SELECT") {
    assume(gteqSpark3_4)
    withTable("t") {
      sql("""
            |CREATE TABLE t (id BIGINT, data STRING)
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'old')")
      val oldLocation = loadTable("t").location().toString
      Seq((2L, "x2"), (3L, "x3")).toDF("id", "data").createOrReplaceTempView("source")

      sql("""
            |CREATE OR REPLACE TABLE t
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '3')
            |AS SELECT * FROM source
            |""".stripMargin)

      val replaced = loadTable("t")
      Assertions.assertEquals(oldLocation, replaced.location().toString)
      Assertions.assertEquals("3", replaced.options().get("bucket"))
      checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq((2L, "x2"), (3L, "x3")).toDF())
    }
  }
}

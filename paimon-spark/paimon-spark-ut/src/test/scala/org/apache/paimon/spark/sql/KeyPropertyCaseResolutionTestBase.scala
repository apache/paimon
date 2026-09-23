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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * `primary-key` and `bucket-key` are plain strings in TBLPROPERTIES, so they used to be matched
 * against the schema exactly while every other identifier in the same statement -- notably the
 * partition columns -- followed `spark.sql.caseSensitive`.
 */
abstract class KeyPropertyCaseResolutionTestBase extends PaimonSparkTestBase {

  test("Paimon DDL: primary-key property resolves like every other identifier") {
    withTable("t") {
      spark.sql("""CREATE TABLE t (Id INT, Name STRING)
                  | TBLPROPERTIES ('primary-key'='id', 'bucket'='1')""".stripMargin)
      // The key is stored under the column's real spelling, not the one the user typed.
      assert(loadTable("t").primaryKeys().asScala == Seq("Id"))

      spark.sql("INSERT INTO t VALUES (1, 'a')")
      spark.sql("INSERT INTO t VALUES (1, 'b')")
      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, "b") :: Nil)
    }
  }

  test("Paimon DDL: bucket-key property resolves like every other identifier") {
    withTable("t") {
      spark.sql("""CREATE TABLE t (Id INT, Name STRING)
                  | TBLPROPERTIES ('bucket-key'='id', 'bucket'='2')""".stripMargin)
      assert(loadTable("t").schema().options().get("bucket-key") == "Id")

      spark.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
      checkAnswer(spark.sql("SELECT * FROM t ORDER BY Id"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("Paimon DDL: multi column and partitioned keys resolve") {
    withTable("t") {
      spark.sql("""CREATE TABLE t (Id INT, Sub INT, Pt STRING, V STRING)
                  | PARTITIONED BY (pt)
                  | TBLPROPERTIES ('primary-key'='id,sub,pt', 'bucket'='1')""".stripMargin)
      assert(loadTable("t").primaryKeys().asScala == Seq("Id", "Sub", "Pt"))
      assert(loadTable("t").partitionKeys().asScala == Seq("Pt"))

      spark.sql("INSERT INTO t VALUES (1, 1, 'p1', 'a')")
      spark.sql("INSERT INTO t VALUES (1, 1, 'p1', 'b')")
      checkAnswer(spark.sql("SELECT V FROM t"), Row("b") :: Nil)
    }
  }

  test("Paimon DDL: ALTER TABLE SET TBLPROPERTIES resolves bucket-key") {
    withTable("t") {
      spark.sql("""CREATE TABLE t (Id INT, Name STRING)
                  | TBLPROPERTIES ('bucket-key'='Id', 'bucket'='2')""".stripMargin)
      spark.sql("ALTER TABLE t SET TBLPROPERTIES ('bucket-key'='id')")
      assert(loadTable("t").schema().options().get("bucket-key") == "Id")
    }
  }

  test("Paimon DDL: an exact spelling keeps working") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (Id INT, Name STRING)
          | TBLPROPERTIES ('primary-key'='Id', 'bucket-key'='Id', 'bucket'='2')""".stripMargin)
      assert(loadTable("t").primaryKeys().asScala == Seq("Id"))
      assert(loadTable("t").schema().options().get("bucket-key") == "Id")
    }
  }

  test("Paimon DDL: an unknown column still reports the original name") {
    withTable("t") {
      val e = intercept[Exception] {
        spark.sql("""CREATE TABLE t (Id INT, Name STRING)
                    | TBLPROPERTIES ('primary-key'='nope', 'bucket'='1')""".stripMargin)
      }
      assert(e.getMessage.contains("nope"), e.getMessage)
    }
  }

  test("Paimon DDL: case sensitive analysis keeps the exact match requirement") {
    withSparkSQLConf("spark.sql.caseSensitive" -> "true") {
      withTable("t") {
        val e = intercept[Exception] {
          spark.sql("""CREATE TABLE t (Id INT, Name STRING)
                      | TBLPROPERTIES ('primary-key'='id', 'bucket'='1')""".stripMargin)
        }
        assert(e.getMessage.contains("id"), e.getMessage)
      }
    }
  }
}

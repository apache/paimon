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

import org.apache.paimon.data.BlobDescriptor
import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.spark.catalog.functions.PaimonFunctions
import org.apache.paimon.spark.function.FunctionResources.MyIntSumClass

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier}

class PaimonFunctionTest extends PaimonHiveTestBase {

  test("Paimon function: list and load functions") {
    Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        {
          sql(s"use $catalogName")
          val functionCatalog =
            spark.sessionState.catalogManager.currentCatalog.asInstanceOf[FunctionCatalog]

          // test load paimon function
          val identifiers = functionCatalog.listFunctions(Array.empty)
          PaimonFunctions.names
            .forEach(
              name => {
                val identifier = identifiers.find(x => x.name().equals(name))
                assert(identifier.isDefined)
                val function = functionCatalog.loadFunction(identifier.get)
                assert(function.name().equals(name))
              })

          // test load fake function
          assertThrows[Throwable] {
            functionCatalog.loadFunction(Identifier.of(Array.empty, "fake_fun"))
          }
        }
    }
  }

  test("Paimon function: show functions") {
    Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        {
          sql(s"use $catalogName")
          assert(sql("show functions").collect().length > 0)
        }
    }
  }

  test("Paimon function: show user functions") {
    assume(gteqSpark3_4)
    Seq("paimon", paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"use $catalogName")
        val functions = sql("show user functions in sys").collect()
        assert(functions.exists(_.getString(0).contains("max_pt")), catalogName)
    }
  }

  test("Paimon function: bucket join with SparkGenericCatalog") {
    sql(s"use $sparkCatalogName")
    assume(gteqSpark3_3)
    withTable("t1", "t2") {
      sql(
        "CREATE TABLE t1 (id INT, c STRING) using paimon TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      sql("INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      sql(
        "CREATE TABLE t2 (id INT, c STRING) using paimon TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      sql("INSERT INTO t2 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswer(
        sql("SELECT * FROM t1 JOIN t2 on t1.id = t2.id order by t1.id"),
        Seq(
          Row(1, "x1", 1, "x1"),
          Row(2, "x3", 2, "x3"),
          Row(3, "x3", 3, "x3"),
          Row(4, "x4", 4, "x4"),
          Row(5, "x5", 5, "x5"))
      )
    }
  }

  test("Paimon function: show and load function with SparkGenericCatalog") {
    sql(s"USE $sparkCatalogName")
    sql(s"USE $hiveDbName")
    sql(s"CREATE FUNCTION myIntSum AS '$MyIntSumClass'")
    checkAnswer(
      sql(s"SHOW FUNCTIONS FROM $hiveDbName LIKE 'myIntSum'"),
      Row("spark_catalog.test_hive.myintsum"))

    withTable("t") {
      sql("CREATE TABLE t (id INT) USING paimon")
      sql("INSERT INTO t VALUES (1), (2), (3)")
      checkAnswer(sql("SELECT myIntSum(id) FROM t"), Row(6))
    }

    sql("DROP FUNCTION myIntSum")
    checkAnswer(sql(s"SHOW FUNCTIONS FROM $hiveDbName LIKE 'myIntSum'"), Seq.empty)
  }

  test("Paimon function: descriptor to presigned URL") {
    sql(s"USE paimon.$dbName0")
    withTable("url_source", "url_other") {
      sql("CREATE TABLE url_source (id INT) USING paimon")
      sql("CREATE TABLE url_other (id INT) USING paimon")

      val descriptor = new BlobDescriptor("file:/tmp/source.blob", 0, 1).serialize()
      val descriptorHex = descriptor.map(b => f"${b & 0xff}%02X").mkString

      checkAnswer(
        sql(
          s"SELECT sys.try_descriptor_to_presigned_url(" +
            s"'$dbName0.url_source', CAST(NULL AS BINARY), 'png', INTERVAL '1' SECOND)"),
        Row(null))
      checkAnswer(
        sql(
          s"SELECT sys.try_descriptor_to_presigned_url(" +
            s"'paimon.$dbName0.url_source', X'00', 'png', INTERVAL '1' HOUR)"),
        Row(null))

      intercept[Exception] {
        sql(
          s"SELECT sys.descriptor_to_presigned_url(" +
            s"'$dbName0.url_source', X'00', 'png', INTERVAL '1' SECOND)").collect()
      }
      intercept[Exception] {
        sql(
          s"SELECT sys.descriptor_to_presigned_url(" +
            s"'$dbName0.url_source', X'$descriptorHex', 'png', " +
            "INTERVAL '1.000001' SECOND)").collect()
      }
      intercept[Exception] {
        sql(
          s"SELECT sys.try_descriptor_to_presigned_url(" +
            s"source_table, X'00', 'png', INTERVAL '1' SECOND) " +
            s"FROM VALUES ('$dbName0.url_source') AS t(source_table)").collect()
      }
      intercept[Exception] {
        sql(
          s"SELECT sys.try_descriptor_to_presigned_url(" +
            s"CASE WHEN id = 1 THEN '$dbName0.url_source' " +
            s"ELSE '$dbName0.url_other' END, X'00', 'png', INTERVAL '1' SECOND) " +
            "FROM VALUES (1) AS t(id)").collect()
      }
      intercept[Exception] {
        sql(
          "SELECT sys.try_descriptor_to_presigned_url(" +
            "CAST(NULL AS STRING), X'00', 'png', INTERVAL '1' SECOND)").collect()
      }
      intercept[Exception] {
        sql(
          s"SELECT sys.try_descriptor_to_presigned_url(" +
            s"'spark_catalog.$dbName0.url_source', X'00', 'png', " +
            "INTERVAL '1' SECOND)").collect()
      }
    }
  }

  test("Add max_pt function") {
    Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        {
          sql(s"use $catalogName")
          val maxPt = if (catalogName == sparkCatalogName) {
            "paimon.sys.max_pt"
          } else {
            "sys.max_pt"
          }

          intercept[Exception] {
            sql(s"SELECT $maxPt(1)").collect()
          }
          intercept[Exception] {
            sql(s"SELECT $maxPt()").collect()
          }
          withTable("t") {
            sql("CREATE TABLE t (id INT) USING paimon")
            intercept[Exception] {
              sql(s"SELECT $maxPt('t')").collect()
            }
          }

          withTable("t") {
            sql("CREATE TABLE t (id INT) USING paimon PARTITIONED BY (p1 STRING)")
            intercept[Exception] {
              sql(s"SELECT $maxPt('t')").collect()
            }
            sql("INSERT INTO t PARTITION (p1='a') VALUES (1)")
            sql("INSERT INTO t PARTITION (p1='b') VALUES (2)")
            sql("INSERT INTO t PARTITION (p1='aa') VALUES (3)")
            sql("ALTER TABLE t ADD PARTITION (p1='z')")
            checkAnswer(sql(s"SELECT $maxPt('t')"), Row("b"))
            checkAnswer(sql(s"SELECT id FROM t WHERE p1 = $maxPt('default.t')"), Row(2))
          }

          withTable("t") {
            sql("CREATE TABLE t (id INT) USING paimon PARTITIONED BY (p1 INT, p2 STRING)")
            intercept[Exception] {
              sql(s"SELECT $maxPt('t')").collect()
            }
            sql("INSERT INTO t PARTITION (p1=1, p2='c') VALUES (1)")
            sql("INSERT INTO t PARTITION (p1=2, p2='a') VALUES (2)")
            sql("INSERT INTO t PARTITION (p1=2, p2='b') VALUES (3)")
            sql("ALTER TABLE t ADD PARTITION (p1='9', p2='z')")
            checkAnswer(sql(s"SELECT $maxPt('t')"), Row("2"))
            checkAnswer(
              sql(s"SELECT id FROM t WHERE p1 = $maxPt('default.t')"),
              Row(2) :: Row(3) :: Nil)
          }
        }
    }
  }
}

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

import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.spark.catalog.functions.PaimonFunctions

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

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
          PaimonFunctions
            .names()
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
    sql("CREATE FUNCTION myIntSum AS 'org.apache.paimon.spark.sql.MyIntSum'")
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
}

private class MyIntSum extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType().add("input", IntegerType)

  override def bufferSchema: StructType = new StructType().add("buffer", IntegerType)

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0)
  }
}

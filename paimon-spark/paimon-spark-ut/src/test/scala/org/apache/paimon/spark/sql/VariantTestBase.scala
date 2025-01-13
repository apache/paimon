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

abstract class VariantTestBase extends PaimonSparkTestBase {

  test("Paimon Variant: read and write variant") {
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql("""
          |INSERT INTO T VALUES
          | (1, parse_json('{"age":26,"city":"Beijing"}')),
          | (2, parse_json('{"age":27,"city":"Hangzhou"}'))
          | """.stripMargin)

    checkAnswer(
      sql(
        "SELECT id, variant_get(v, '$.age', 'int'), variant_get(v, '$.city', 'string') FROM T ORDER BY id"),
      Seq(Row(1, 26, "Beijing"), Row(2, 27, "Hangzhou"))
    )
    checkAnswer(
      sql(
        "SELECT variant_get(v, '$.city', 'string') FROM T WHERE variant_get(v, '$.age', 'int') == 26"),
      Seq(Row("Beijing"))
    )
    checkAnswer(
      sql("SELECT * FROM T WHERE variant_get(v, '$.age', 'int') == 27"),
      sql("""SELECT 2, parse_json('{"age":27,"city":"Hangzhou"}')""")
    )
  }

  test("Paimon Variant: read and write array variant") {
    sql("CREATE TABLE T (id INT, v ARRAY<VARIANT>)")
    sql(
      """
        |INSERT INTO T VALUES
        | (1, array(parse_json('{"age":26,"city":"Beijing"}'), parse_json('{"age":27,"city":"Hangzhou"}'))),
        | (2, array(parse_json('{"age":27,"city":"Shanghai"}')))
        | """.stripMargin)

    withSparkSQLConf("spark.sql.ansi.enabled" -> "false") {
      checkAnswer(
        sql(
          "SELECT id, variant_get(v[1], '$.age', 'int'), variant_get(v[0], '$.city', 'string') FROM T ORDER BY id"),
        Seq(Row(1, 27, "Beijing"), Row(2, null, "Shanghai"))
      )
    }
  }

  test("Paimon Variant: complex json") {
    val json =
      """
        |{
        |  "object" : {
        |    "name" : "Apache Paimon",
        |    "age" : 2,
        |    "address" : {
        |      "street" : "Main St",
        |      "city" : "Hangzhou"
        |    }
        |  },
        |  "array" : [ 1, 2, 3, 4, 5 ],
        |  "string" : "Hello, World!",
        |  "long" : 12345678901234,
        |  "double" : 1.0123456789012346,
        |  "decimal" : 100.99,
        |  "boolean1" : true,
        |  "boolean2" : false,
        |  "nullField" : null
        |}
        |""".stripMargin

    sql("CREATE TABLE T (v VARIANT)")
    sql(s"""
           |INSERT INTO T VALUES parse_json('$json')
           | """.stripMargin)

    checkAnswer(
      sql("""
            |SELECT
            | variant_get(v, '$.object', 'string'),
            | variant_get(v, '$.object.name', 'string'),
            | variant_get(v, '$.object.address.street', 'string'),
            | variant_get(v, '$["object"]["address"].city', 'string'),
            | variant_get(v, '$.array', 'string'),
            | variant_get(v, '$.array[0]', 'int'),
            | variant_get(v, '$.array[3]', 'int'),
            | variant_get(v, '$.string', 'string'),
            | variant_get(v, '$.double', 'double'),
            | variant_get(v, '$.decimal', 'decimal(5,2)'),
            | variant_get(v, '$.boolean1', 'boolean'),
            | variant_get(v, '$.boolean2', 'boolean'),
            | variant_get(v, '$.nullField', 'string')
            |FROM T
            |""".stripMargin),
      Seq(
        Row(
          """{"address":{"city":"Hangzhou","street":"Main St"},"age":2,"name":"Apache Paimon"}""",
          "Apache Paimon",
          "Main St",
          "Hangzhou",
          "[1,2,3,4,5]",
          1,
          4,
          "Hello, World!",
          1.0123456789012346,
          100.99,
          true,
          false,
          null
        ))
    )
  }
}

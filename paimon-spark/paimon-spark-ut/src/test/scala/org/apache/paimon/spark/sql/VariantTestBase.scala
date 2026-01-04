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

  test("Paimon Variant: read and write shredded variant") {
    sql(
      """
        |CREATE TABLE T (id INT, v VARIANT)
        |TBLPROPERTIES
        |('parquet.variant.shreddingSchema' =
        |'{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW","fields":[{"name":"age","type":"INT"},{"name":"city","type":"STRING"}]}}]}'
        |)
        |""".stripMargin)

    val values =
      """
        | SELECT
        | id,
        | CASE
        | WHEN id = 0 THEN parse_json('{"age":27,"city":"Beijing"}')
        | WHEN id = 1 THEN parse_json('{"age":27}')
        | WHEN id = 2 THEN parse_json('{"city":"Beijing", "other":"xxx"}')
        | WHEN id = 3 THEN parse_json('{"other":"yyy"}')
        | WHEN id = 4 THEN parse_json('{"age":"27"}')
        | WHEN id = 5 THEN parse_json('"zzz"')
        | WHEN id = 6 THEN parse_json('{}')
        | END v FROM range(7)
        |""".stripMargin

    sql(s"INSERT INTO T $values")

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), sql(values))

    checkAnswer(
      sql("SELECT variant_get(v, '$.age', 'int') FROM T ORDER BY id"),
      Seq(Row(27), Row(27), Row(null), Row(null), Row(27), Row(null), Row(null))
    )

    checkAnswer(
      sql(
        "SELECT variant_get(v, '$.city', 'string') FROM T where variant_get(v, '$.age', 'int') = 27 ORDER BY id"),
      Seq(Row("Beijing"), Row(null), Row(null))
    )
  }

  test("Paimon Variant: read and write shredded variant with all types") {
    for (isPkTable <- Seq(true, false)) {
      val pkProps = if (isPkTable) "'primary-key' = 'id'," else ""
      withTable("t") {
        sql(s"""
               |CREATE TABLE t (id INT, v VARIANT) TBLPROPERTIES
               |(
               | $pkProps
               |'parquet.variant.shreddingSchema' =
               |'{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW",
               |"fields":
               |  [ {
               |    "name" : "object_col",
               |    "type" : {
               |      "type" : "ROW",
               |      "fields" : [ {
               |        "name" : "name",
               |        "type" : "STRING"
               |      }, {
               |        "name" : "age",
               |        "type" : "INT"
               |      } ]
               |    }
               |  }, {
               |    "name" : "array_col",
               |    "type" : {
               |      "type" : "ARRAY",
               |      "element" : "INT"
               |    }
               |  }, {
               |    "name" : "string_col",
               |    "type" : "STRING"
               |  }, {
               |    "name" : "byte_col",
               |    "type" : "TINYINT"
               |  }, {
               |    "name" : "short_col",
               |    "type" : "SMALLINT"
               |  }, {
               |    "name" : "int_col",
               |    "type" : "INT"
               |  }, {
               |    "name" : "long_col",
               |    "type" : "BIGINT"
               |  }, {
               |    "name" : "float_col",
               |    "type" : "FLOAT"
               |  }, {
               |    "name" : "double_col",
               |    "type" : "DOUBLE"
               |  }, {
               |    "name" : "decimal_col",
               |    "type" : "DECIMAL(5, 2)"
               |  }, {
               |    "name" : "boolean_col",
               |    "type" : "BOOLEAN"
               |  } ]
               |}}]}'
               |)
               |""".stripMargin)

        val json1 =
          """
            |{
            |  "object_col": {
            |    "name": "Apache Paimon",
            |    "age": 3
            |  },
            |  "array_col": [1, 2, 3, 4, 5],
            |  "string_col": "hello",
            |  "byte_col": 1,
            |  "short_col": 3000,
            |  "int_col": 40000,
            |  "long_col": 12345678901234,
            |  "float_col": 5.2,
            |  "double_col": 1.012345678901,
            |  "decimal_col": 100.99,
            |  "boolean_col": true
            |}
            |""".stripMargin

        val json2 =
          """
            |{
            |  "object_col": {
            |    "name": "Tom",
            |    "age": 35
            |  },
            |  "array_col": [6, 7, 8],
            |  "string_col": "hi",
            |  "byte_col": 2,
            |  "short_col": 4000,
            |  "int_col": 50000,
            |  "long_col": 62345678901234,
            |  "float_col": 7.2,
            |  "double_col": 2.012345678901,
            |  "decimal_col": 111.99,
            |  "boolean_col": false
            |}
            |""".stripMargin

        sql(
          s"""
             |INSERT INTO t
             | SELECT
             | /*+ REPARTITION(1) */
             | id,
             | CASE
             | WHEN id = 0 THEN parse_json('$json1')
             | WHEN id = 1 THEN parse_json('$json2')
             | END v
             | FROM range(2)
             |""".stripMargin
        )

        checkAnswer(
          sql("SELECT id, CAST(v AS STRING) FROM t ORDER BY id"),
          Seq(
            Row(
              0,
              """{"array_col":[1,2,3,4,5],"boolean_col":true,"byte_col":1,"decimal_col":100.99,"double_col":1.012345678901,"float_col":5.2,"int_col":40000,"long_col":12345678901234,"object_col":{"age":3,"name":"Apache Paimon"},"short_col":3000,"string_col":"hello"}"""
            ),
            Row(
              1,
              """{"array_col":[6,7,8],"boolean_col":false,"byte_col":2,"decimal_col":111.99,"double_col":2.012345678901,"float_col":7.2,"int_col":50000,"long_col":62345678901234,"object_col":{"age":35,"name":"Tom"},"short_col":4000,"string_col":"hi"}"""
            )
          )
        )

        checkAnswer(
          sql("""
                |SELECT
                |variant_get(v, '$.object_col', 'struct<name string, age int>'),
                |variant_get(v, '$.object_col.name', 'string'),
                |variant_get(v, '$.array_col', 'array<int>'),
                |variant_get(v, '$.array_col[2]', 'int'),
                |variant_get(v, '$.array_col[3]', 'int'),
                |variant_get(v, '$.string_col', 'string'),
                |variant_get(v, '$.byte_col', 'byte'),
                |variant_get(v, '$.short_col', 'short'),
                |variant_get(v, '$.int_col', 'int'),
                |variant_get(v, '$.long_col', 'long'),
                |variant_get(v, '$.float_col', 'float'),
                |variant_get(v, '$.double_col', 'double'),
                |variant_get(v, '$.boolean_col', 'boolean'),
                |variant_get(v, '$.decimal_col', 'decimal(5, 2)')
                |FROM t ORDER BY id
                |""".stripMargin),
          Seq(
            Row(
              Row("Apache Paimon", 3),
              "Apache Paimon",
              Array(1, 2, 3, 4, 5),
              3,
              4,
              "hello",
              1.toByte,
              3000.toShort,
              40000,
              12345678901234L,
              5.2f,
              1.012345678901d,
              true,
              BigDecimal.apply("100.99")
            ),
            Row(
              Row("Tom", 35),
              "Tom",
              Array(6, 7, 8),
              8,
              null,
              "hi",
              2.toByte,
              4000.toShort,
              50000,
              62345678901234L,
              7.2f,
              2.012345678901d,
              false,
              BigDecimal.apply("111.99"))
          )
        )
      }
    }
  }
}

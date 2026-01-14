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

  test("Paimon Variant: read and write shredded and unshredded variant") {
    sql(
      """
        |CREATE TABLE T
        |(id INT, v1 VARIANT, v2 VARIANT, v3 VARIANT)
        |TBLPROPERTIES
        |('parquet.variant.shreddingSchema' =
        |'{"type":"ROW","fields":[{"name":"v1","type":{"type":"ROW","fields":[{"name":"age","type":"INT"},{"name":"city","type":"STRING"}]}}]}'
        |)
        |""".stripMargin)
    sql(
      """
        |INSERT INTO T VALUES
        | (1, parse_json('{"age":26,"city":"Beijing"}'), parse_json('{"age":26,"city":"Beijing"}'), parse_json('{"age":26,"city":"Beijing"}'))
        | """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM T"),
      sql(
        """SELECT 1, parse_json('{"age":26,"city":"Beijing"}'), parse_json('{"age":26,"city":"Beijing"}'), parse_json('{"age":26,"city":"Beijing"}')""")
    )

    checkAnswer(
      sql(
        "SELECT variant_get(v1, '$.age', 'int'), variant_get(v2, '$.age', 'int'), variant_get(v3, '$.age', 'int') FROM T"),
      Seq(Row(26, 26, 26))
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

  test("Paimon Variant: read and write variant with null value") {
    withTable("source_tbl", "target_tbl") {
      sql("CREATE TABLE source_tbl (id INT, js STRING) USING paimon")
      val n = 100
      val nullCount = 98
      val values = (1 to n)
        .map {
          i =>
            if (i <= nullCount) {
              s"($i, null)"
            } else {
              val jsonStr =
                s"""
                   |'{
                   |  "id":$i,"name":"user$i","age":${20 + (i % 50)},
                   |  "tags":[{"type":"vip","level":$i},{"type":"premium","level":$i}],
                   |  "address":{"city":"city$i","street":"street$i"}
                   |}'
                   |""".stripMargin
              s"($i, $jsonStr)"
            }
        }
        .mkString(", ")
      sql(s"INSERT INTO source_tbl VALUES $values")

      sql("CREATE TABLE target_tbl (id INT, v VARIANT) USING paimon")
      sql("INSERT INTO target_tbl SELECT id, parse_json(js) FROM source_tbl")

      checkAnswer(
        sql("""
              |SELECT
              |variant_get(v, '$.name', 'string'),
              |variant_get(v, '$.tags', 'string'),
              |variant_get(v, '$.tags', 'array<string>'),
              |variant_get(v, '$.tags', 'array<struct<type string, level int>>'),
              |variant_get(v, '$.tags[0]', 'string'),
              |variant_get(v, '$.tags[0]', 'struct<type string, level int>'),
              |variant_get(v, '$.tags[1].type', 'string'),
              |variant_get(v, '$.address', 'string')
              |FROM target_tbl where v IS NOT NULL
              |""".stripMargin),
        Seq(
          Row(
            "user99",
            "[{\"level\":99,\"type\":\"vip\"},{\"level\":99,\"type\":\"premium\"}]",
            Array("{\"level\":99,\"type\":\"vip\"}", "{\"level\":99,\"type\":\"premium\"}"),
            Array(Row("vip", 99), Row("premium", 99)),
            "{\"level\":99,\"type\":\"vip\"}",
            Row("vip", 99),
            "premium",
            "{\"city\":\"city99\",\"street\":\"street99\"}"
          ),
          Row(
            "user100",
            "[{\"level\":100,\"type\":\"vip\"},{\"level\":100,\"type\":\"premium\"}]",
            Array("{\"level\":100,\"type\":\"vip\"}", "{\"level\":100,\"type\":\"premium\"}"),
            Array(Row("vip", 100), Row("premium", 100)),
            "{\"level\":100,\"type\":\"vip\"}",
            Row("vip", 100),
            "premium",
            "{\"city\":\"city100\",\"street\":\"street100\"}"
          )
        )
      )
    }
  }

  test("Paimon Variant: edge case json - empty objects and arrays") {
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql("""
          |INSERT INTO T VALUES
          | (1, parse_json('{}')),
          | (2, parse_json('[]')),
          | (3, parse_json('{"empty_obj":{},"empty_arr":[]}')),
          | (4, parse_json('{"nested":{"deep":{"empty":{}}}}')),
          | (5, parse_json('[[[]]]]'))
          | """.stripMargin)

    val expectedSelect = sql("""
                               |SELECT 1, parse_json('{}') UNION ALL
                               |SELECT 2, parse_json('[]') UNION ALL
                               |SELECT 3, parse_json('{"empty_obj":{},"empty_arr":[]}') UNION ALL
                               |SELECT 4, parse_json('{"nested":{"deep":{"empty":{}}}}') UNION ALL
                               |SELECT 5, parse_json('[[[]]]]')
                               |""".stripMargin)

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), expectedSelect)

    checkAnswer(
      sql("""
            |SELECT id,
            |variant_get(v, '$.empty_obj', 'string'),
            |variant_get(v, '$.empty_arr', 'string'),
            |variant_get(v, '$.nested.deep.empty', 'string')
            |FROM T ORDER BY id
            |""".stripMargin),
      Seq(
        Row(1, null, null, null),
        Row(2, null, null, null),
        Row(3, "{}", "[]", null),
        Row(4, null, null, "{}"),
        Row(5, null, null, null)
      )
    )
  }

  test("Paimon Variant: edge case json - special characters and unicode") {
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql("""
          |INSERT INTO T VALUES
          | (1, parse_json('{"key":"value with \\"quotes\\""}')),
          | (2, parse_json('{"key":"line1\\nline2"}')),
          | (3, parse_json('{"key":"tab\\there"}')),
          | (4, parse_json('{"key":"backslash\\\\test"}')),
          | (5, parse_json('{"chinese":"中文测试","emoji":"😀🎉"}')),
          | (6, parse_json('{"special":"!@#$%^&*()_+-={}[]|:;<>?,./"}'))
          | """.stripMargin)

    val expectedSelect =
      sql("""
            |SELECT 1, parse_json('{"key":"value with \\"quotes\\""}') UNION ALL
            |SELECT 2, parse_json('{"key":"line1\\nline2"}') UNION ALL
            |SELECT 3, parse_json('{"key":"tab\\there"}') UNION ALL
            |SELECT 4, parse_json('{"key":"backslash\\\\test"}') UNION ALL
            |SELECT 5, parse_json('{"chinese":"中文测试","emoji":"😀🎉"}') UNION ALL
            |SELECT 6, parse_json('{"special":"!@#$%^&*()_+-={}[]|:;<>?,./"}')  
            |""".stripMargin)

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), expectedSelect)

    checkAnswer(
      sql("""
            |SELECT id,
            |variant_get(v, '$.key', 'string'),
            |variant_get(v, '$.chinese', 'string'),
            |variant_get(v, '$.emoji', 'string'),
            |variant_get(v, '$.special', 'string')
            |FROM T ORDER BY id
            |""".stripMargin),
      Seq(
        Row(1, "value with \"quotes\"", null, null, null),
        Row(2, "line1\nline2", null, null, null),
        Row(3, "tab\there", null, null, null),
        Row(4, "backslash\\test", null, null, null),
        Row(5, null, "中文测试", "😀🎉", null),
        Row(6, null, null, null, "!@#$%^&*()_+-={}[]|:;<>?,./")
      )
    )
  }

  test("Paimon Variant: edge case json - extreme numeric values") {
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql("""
          |INSERT INTO T VALUES
          | (1, parse_json('{"max_long":9223372036854775807}')),
          | (2, parse_json('{"min_long":-9223372036854775808}')),
          | (3, parse_json('{"zero":0}')),
          | (4, parse_json('{"neg_zero":-0}')),
          | (5, parse_json('{"large_decimal":123456789012345678901234567890.123456789}')),
          | (6, parse_json('{"scientific":1.23e10}')),
          | (7, parse_json('{"neg_scientific":-4.56e-7}'))
          | """.stripMargin)

    val expectedSelect = sql(
      """
        |SELECT 1 AS id, parse_json('{"max_long":9223372036854775807}') AS v UNION ALL
        |SELECT 2, parse_json('{"min_long":-9223372036854775808}') UNION ALL
        |SELECT 3, parse_json('{"zero":0}') UNION ALL
        |SELECT 4, parse_json('{"neg_zero":-0}') UNION ALL
        |SELECT 5, parse_json('{"large_decimal":123456789012345678901234567890.123456789}') UNION ALL
        |SELECT 6, parse_json('{"scientific":1.23e10}') UNION ALL
        |SELECT 7, parse_json('{"neg_scientific":-4.56e-7}')
        |""".stripMargin)

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), expectedSelect)

    checkAnswer(
      sql("""
            |SELECT id,
            |variant_get(v, '$.max_long', 'long'),
            |variant_get(v, '$.min_long', 'long'),
            |variant_get(v, '$.zero', 'int'),
            |variant_get(v, '$.neg_zero', 'int'),
            |variant_get(v, '$.large_decimal', 'double'),
            |variant_get(v, '$.scientific', 'double'),
            |variant_get(v, '$.neg_scientific', 'double')
            |FROM T ORDER BY id
            |""".stripMargin),
      Seq(
        Row(1, 9223372036854775807L, null, null, null, null, null, null),
        Row(2, null, -9223372036854775808L, null, null, null, null, null),
        Row(3, null, null, 0, null, null, null, null),
        Row(4, null, null, null, 0, null, null, null),
        Row(5, null, null, null, null, 1.2345678901234568e29, null, null),
        Row(6, null, null, null, null, null, 1.23e10, null),
        Row(7, null, null, null, null, null, null, -4.56e-7)
      )
    )
  }

  test("Paimon Variant: edge case json - deeply nested structures") {
    val deepJson = {
      val opens = (1 to 10).map(i => s""""level$i":{""").mkString
      val closes = "}" * 10
      s"""{$opens"value":"deep"$closes}"""
    }
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql(s"""
           |INSERT INTO T VALUES
           | (1, parse_json('$deepJson')),
           | (2, parse_json('{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":100}}}}}}}}}}')),
           | (3, parse_json('[[[[[[[[[10]]]]]]]]]'))
           | """.stripMargin)

    val expectedSelect = sql(
      s"""
         |SELECT 1 AS id, parse_json('$deepJson') AS v UNION ALL
         |SELECT 2, parse_json('{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":100}}}}}}}}}}') UNION ALL
         |SELECT 3, parse_json('[[[[[[[[[10]]]]]]]]]')
         |""".stripMargin)

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), expectedSelect)

    checkAnswer(
      sql("""
            |SELECT id,
            |variant_get(v, '$.level1.level2.level3.level4.level5.level6.level7.level8.level9.level10.value', 'string'),
            |variant_get(v, '$.a.b.c.d.e.f.g.h.i.j', 'int'),
            |variant_get(v, '$[0][0][0][0][0][0][0][0][0]', 'int')
            |FROM T ORDER BY id
            |""".stripMargin),
      Seq(
        Row(1, "deep", null, null),
        Row(2, null, 100, null),
        Row(3, null, null, 10)
      )
    )
  }

  test("Paimon Variant: edge case json - mixed types in arrays") {
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql("""
          |INSERT INTO T VALUES
          | (1, parse_json('{"mixed":[1, "two", true, null, {"key":"value"}, [7,8,9]]}')),
          | (2, parse_json('{"numbers":[1, 2.5, 1e10, -100]}')),
          | (3, parse_json('{"nested":[{"a":1},{"b":2},{"c":{"d":3}}]}'))
          | """.stripMargin)

    val expectedSelect = sql(
      """
        |SELECT 1 AS id, parse_json('{"mixed":[1, "two", true, null, {"key":"value"}, [7,8,9]]}') AS v UNION ALL
        |SELECT 2, parse_json('{"numbers":[1, 2.5, 1e10, -100]}') UNION ALL
        |SELECT 3, parse_json('{"nested":[{"a":1},{"b":2},{"c":{"d":3}}]}')
        |""".stripMargin)

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), expectedSelect)

    checkAnswer(
      sql("""
            |SELECT id,
            |variant_get(v, '$.mixed[0]', 'int'),
            |variant_get(v, '$.mixed[1]', 'string'),
            |variant_get(v, '$.mixed[2]', 'boolean'),
            |variant_get(v, '$.mixed[4].key', 'string'),
            |variant_get(v, '$.mixed[5][1]', 'int'),
            |variant_get(v, '$.numbers[2]', 'double'),
            |variant_get(v, '$.nested[2].c.d', 'int')
            |FROM T ORDER BY id
            |""".stripMargin),
      Seq(
        Row(1, 1, "two", true, "value", 8, null, null),
        Row(2, null, null, null, null, null, 1e10, null),
        Row(3, null, null, null, null, null, null, 3)
      )
    )
  }

  test("Paimon Variant: primitive types as variant") {
    sql("CREATE TABLE T (id INT, v VARIANT)")
    sql("""
          |INSERT INTO T VALUES
          | (1, CAST(42 AS VARIANT)),
          | (2, CAST(-99 AS VARIANT)),
          | (3, CAST(9223372036854775807 AS VARIANT)),
          | (4, CAST(3.14 AS VARIANT)),
          | (5, CAST(1.23e10 AS VARIANT)),
          | (6, CAST('hello' AS VARIANT)),
          | (7, CAST('' AS VARIANT)),
          | (8, CAST(true AS VARIANT)),
          | (9, CAST(false AS VARIANT)),
          | (10, CAST(null AS VARIANT))
          | """.stripMargin)

    val expectedSelect = sql("""
                               |SELECT 1 AS id, CAST(42 AS VARIANT) AS v UNION ALL
                               |SELECT 2, CAST(-99 AS VARIANT) UNION ALL
                               |SELECT 3, CAST(9223372036854775807 AS VARIANT) UNION ALL
                               |SELECT 4, CAST(3.14 AS VARIANT) UNION ALL
                               |SELECT 5, CAST(1.23e10 AS VARIANT) UNION ALL
                               |SELECT 6, CAST('hello' AS VARIANT) UNION ALL
                               |SELECT 7, CAST('' AS VARIANT) UNION ALL
                               |SELECT 8, CAST(true AS VARIANT) UNION ALL
                               |SELECT 9, CAST(false AS VARIANT) UNION ALL
                               |SELECT 10, CAST(null AS VARIANT)
                               |""".stripMargin)

    checkAnswer(sql("SELECT * FROM T ORDER BY id"), expectedSelect)

    checkAnswer(
      sql("""
            |SELECT id,
            |variant_get(v, '$', 'string')
            |FROM T ORDER BY id
            |""".stripMargin),
      Seq(
        Row(1, "42"),
        Row(2, "-99"),
        Row(3, "9223372036854775807"),
        Row(4, "3.14"),
        Row(5, "1.23E10"),
        Row(6, "hello"),
        Row(7, ""),
        Row(8, "true"),
        Row(9, "false"),
        Row(10, null)
      )
    )
  }
}

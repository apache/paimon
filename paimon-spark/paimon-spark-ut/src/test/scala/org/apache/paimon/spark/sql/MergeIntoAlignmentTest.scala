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

class MergeIntoAlignmentTest extends PaimonSparkTestBase {

  test("basic merge: matched UPDATE *, not-matched INSERT *") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      spark
        .sql("SELECT 1 AS id, 'A' AS name UNION ALL SELECT 3 AS id, 'c' AS name")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET *
            | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(1, "A"), Row(2, "b"), Row(3, "c"))
      )
    }
  }

  // Source top-level extras are silently dropped under star expansion when merge-schema
  // is off; `*` expands over target columns.
  test(
    "merge into: source extra columns silently dropped under star expansion (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a')")

      spark
        .sql("""SELECT 1 AS id, 'A' AS name, 'upd' AS op
               | UNION ALL SELECT 2 AS id, 'b' AS name, 'ins' AS op""".stripMargin)
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET *
            | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(1, "A"), Row(2, "b"))
      )
    }
  }

  // With merge-schema enabled, the source-extra column is evolved into the target schema.
  test("merge into: source extra columns evolve target schema (mergeSchema=true)") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (id INT, name STRING)
              | USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
        sql("INSERT INTO t VALUES (1, 'a')")

        spark
          .sql("""SELECT 1 AS id, 'A' AS name, 'upd' AS op
                 | UNION ALL SELECT 2 AS id, 'b' AS name, 'ins' AS op""".stripMargin)
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

        checkAnswer(
          sql("SELECT id, name, op FROM t ORDER BY id"),
          Seq(Row(1, "A", "upd"), Row(2, "b", "ins"))
        )
      }
    }
  }

  test("merge into: target has extra column missing in source") {
    withTable("news") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE news (
              |  newsId STRING,
              |  newsName STRING,
              |  tenantIds ARRAY<STRING>
              |) USING paimon
              | TBLPROPERTIES ('primary-key' = 'newsId', 'bucket' = '1')""".stripMargin)
        sql("INSERT INTO news VALUES ('n0', 'old', array('t1'))")

        spark
          .sql("""SELECT 'n0' AS newsId, 'updated' AS newsName
                 | UNION ALL SELECT 'n1' AS newsId, 'foo' AS newsName""".stripMargin)
          .createOrReplaceTempView("checkStatus")

        sql("""MERGE INTO news a
              | USING checkStatus b ON a.newsId = b.newsId
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

        checkAnswer(
          sql("SELECT newsId, newsName, tenantIds FROM news ORDER BY newsId"),
          Seq(
            Row("n0", "updated", Seq("t1")),
            Row("n1", "foo", null)
          )
        )
      }
    }
  }

  // Strict default: `UPDATE *` with a top-level target column missing in source throws.
  test("merge into: UPDATE * top-level missing source col throws (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING, score INT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a', 10)")

      spark.sql("SELECT 1 AS id, 'A' AS name").createOrReplaceTempView("s")

      val ex = intercept[Exception] {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }
      assert(ex.getMessage.toLowerCase.contains("score"))
    }
  }

  // Strict default: `INSERT *` with a top-level target column missing in source throws.
  test("merge into: INSERT * top-level missing source col throws (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING, score INT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

      spark.sql("SELECT 2 AS id, 'b' AS name").createOrReplaceTempView("s")

      val ex = intercept[Exception] {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }
      assert(ex.getMessage.toLowerCase.contains("score"))
    }
  }

  // Explicit `INSERT (col list)` continues to NULL-fill unmentioned target columns (standard SQL).
  test("merge into: explicit INSERT (col list) NULL-fills unmentioned target columns") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING, score INT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

      spark.sql("SELECT 2 AS id, 'b' AS name").createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)""".stripMargin)

      checkAnswer(sql("SELECT id, name, score FROM t"), Seq(Row(2, "b", null)))
    }
  }

  // UPDATE * with a nested struct value whose source struct is *narrower* than the target struct
  // (source has `a` only; target has `a` and `b`). Under `merge-schema=true`, the source-missing
  // leaf preserves the target's current value.
  test("merge into: UPDATE * nested struct, source missing a leaf preserves target leaf") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (
              |  id INT,
              |  info STRUCT<a: STRING, b: STRING>
              |) USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
        sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA', 'b', 'oldB'))")

        spark
          .sql("""SELECT 1 AS id, named_struct('a', 'newA') AS info""")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

        checkAnswer(
          sql("SELECT id, info.a, info.b FROM t"),
          Seq(Row(1, "newA", "oldB"))
        )
      }
    }
  }

  // Strict default: source struct narrower than target throws.
  test(
    "merge into: UPDATE * nested struct, source missing target field throws (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  id INT,
            |  info STRUCT<a: STRING, b: STRING>
            |) USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA', 'b', 'oldB'))")

      spark
        .sql("""SELECT 1 AS id, named_struct('a', 'newA') AS info""")
        .createOrReplaceTempView("s")

      val ex = intercept[Exception] {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }
      assert(ex.getMessage.toLowerCase.contains("info") || ex.getMessage.toLowerCase.contains("b"))
    }
  }

  // Strict default: source struct wider than target throws — silently dropping would lose data.
  test("merge into: UPDATE * nested struct, source extra field throws (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  id INT,
            |  info STRUCT<a: STRING>
            |) USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA'))")

      spark
        .sql("""SELECT 1 AS id, named_struct('a', 'newA', 'b', 'newB') AS info""")
        .createOrReplaceTempView("s")

      val ex = intercept[Exception] {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }
      assert(ex.getMessage.toLowerCase.contains("info") || ex.getMessage.toLowerCase.contains("b"))
    }
  }

  // With schema evolution on, source-wider nested case evolves the target nested struct.
  test("merge into: UPDATE * nested struct, source extra field evolves schema (mergeSchema=true)") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (
              |  id INT,
              |  info STRUCT<a: STRING>
              |) USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
        sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA'))")

        spark
          .sql("""SELECT 1 AS id, named_struct('a', 'newA', 'b', 'newB') AS info""")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

        checkAnswer(sql("SELECT id, info.a, info.b FROM t"), Seq(Row(1, "newA", "newB")))
      }
    }
  }

  // UPDATE * with a nested struct value that is entirely NULL at source. `fixNullExpansion`
  // should collapse the constructed `named_struct(null, current_b)` to a NULL struct when the
  // target struct is also nullable.
  test("merge into: UPDATE * nested struct, NULL source collapses to NULL struct") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  id INT,
            |  info STRUCT<a: STRING, b: STRING>
            |) USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA', 'b', 'oldB'))")

      spark
        .sql("""SELECT 1 AS id, CAST(NULL AS STRUCT<a: STRING, b: STRING>) AS info""")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

      checkAnswer(
        sql("SELECT id, info FROM t"),
        Seq(Row(1, null))
      )
    }
  }

  // mergeSchema=true PreserveTarget path: source NULL must still collapse to NULL when
  // source/target struct schemas match exactly (no target-only fields to preserve).
  // Without the hasExtraTargetFields check the per-field expansion produces struct(null, null).
  test(
    "merge into: UPDATE * nested struct matching schemas, NULL source collapses (mergeSchema=true)") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (
              |  id INT,
              |  info STRUCT<a: STRING, b: STRING>
              |) USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
        sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA', 'b', 'oldB'))")

        spark
          .sql("""SELECT 1 AS id, CAST(NULL AS STRUCT<a: STRING, b: STRING>) AS info""")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

        checkAnswer(
          sql("SELECT id, info FROM t"),
          Seq(Row(1, null))
        )
      }
    }
  }

  // Strict default: INSERT * with a nested struct narrower than the target throws — uniform with
  // the explicit struct-assignment / `UPDATE *` paths.
  test("merge into: INSERT * nested struct, source missing a leaf throws (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  id INT,
            |  info STRUCT<a: STRING, b: STRING>
            |) USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

      spark
        .sql("""SELECT 2 AS id, named_struct('a', 'newA') AS info""")
        .createOrReplaceTempView("s")

      val ex = intercept[Exception] {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }
      assert(ex.getMessage.toLowerCase.contains("info") || ex.getMessage.toLowerCase.contains("b"))
    }
  }

  // INSERT has no existing target row to preserve, so source-missing nested leaf is NULL-filled
  // when merge-schema is enabled.
  test("merge into: INSERT * nested struct, source missing a leaf gets NULL (mergeSchema=true)") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (
              |  id INT,
              |  info STRUCT<a: STRING, b: STRING>
              |) USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

        spark
          .sql("""SELECT 2 AS id, named_struct('a', 'newA') AS info""")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

        checkAnswer(
          sql("SELECT id, info.a, info.b FROM t"),
          Seq(Row(2, "newA", null))
        )
      }
    }
  }

  // UPDATE * with type coercion (source INT for a BIGINT column). Alignment must inject a Cast.
  test("merge into: UPDATE * with type coercion (INT -> BIGINT)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, cnt BIGINT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 100L)")

      spark
        .sql("SELECT 1 AS id, CAST(42 AS INT) AS cnt")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

      checkAnswer(sql("SELECT id, cnt FROM t"), Seq(Row(1, 42L)))
    }
  }

  // Explicit SET clause (not star): unspecified target columns preserve their current value.
  test("merge into: explicit SET preserves unmentioned target columns") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING, score INT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a', 10)")

      spark.sql("SELECT 1 AS id, 'A' AS name").createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET t.name = s.name""".stripMargin)

      checkAnswer(sql("SELECT id, name, score FROM t"), Seq(Row(1, "A", 10)))
    }
  }

  // Mixed actions: DELETE on one match, UPDATE on another, INSERT for unmatched.
  test("merge into: mixed DELETE + UPDATE + INSERT actions") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      spark
        .sql("""SELECT 1 AS id, 'A' AS name, 'del' AS op
               | UNION ALL SELECT 2 AS id, 'B' AS name, 'upd' AS op
               | UNION ALL SELECT 4 AS id, 'd' AS name, 'ins' AS op""".stripMargin)
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED AND s.op = 'del' THEN DELETE
            | WHEN MATCHED AND s.op = 'upd' THEN UPDATE SET t.name = s.name
            | WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)""".stripMargin)

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(2, "B"), Row(3, "c"), Row(4, "d"))
      )
    }
  }

  // 3-level nested struct under UPDATE *. Innermost leaf missing in source preserves target value
  // at every depth under `merge-schema=true`.
  test("merge into: UPDATE * 3-level nested struct preserves deep leaves") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (
              |  id INT,
              |  l1 STRUCT<a: STRING, l2: STRUCT<b: STRING, l3: STRUCT<c: STRING, d: STRING>>>
              |) USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
        sql("""INSERT INTO t VALUES (
              |  1,
              |  named_struct(
              |    'a', 'oldA',
              |    'l2', named_struct(
              |      'b', 'oldB',
              |      'l3', named_struct('c', 'oldC', 'd', 'oldD')))
              |)""".stripMargin)

        // Source omits `l1.l2.l3.d`; we expect `oldD` to be preserved.
        spark
          .sql("""SELECT 1 AS id,
                 | named_struct(
                 |   'a', 'newA',
                 |   'l2', named_struct(
                 |     'b', 'newB',
                 |     'l3', named_struct('c', 'newC'))) AS l1""".stripMargin)
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

        checkAnswer(
          sql("SELECT id, l1.a, l1.l2.b, l1.l2.l3.c, l1.l2.l3.d FROM t"),
          Seq(Row(1, "newA", "newB", "newC", "oldD"))
        )
      }
    }
  }

  // Explicit `INSERT (newCol) VALUES (s.newCol)` should evolve the target schema under
  // `merge-schema=true` — same outcome as `INSERT *`. The resolver fallback marks the key as
  // source-bound; `MergeSchemaEvolutionHelper` rebinds it onto the evolved target attribute.
  test("merge into: explicit INSERT (newCol) evolves target schema (mergeSchema=true)") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (id INT, name STRING)
              | USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

        spark
          .sql("SELECT 2 AS id, 'b' AS name, 99 AS score")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)
              |""".stripMargin)

        checkAnswer(
          sql("SELECT id, name, score FROM t ORDER BY id"),
          Seq(Row(2, "b", 99))
        )
      }
    }
  }

  // Explicit `UPDATE SET newCol = s.x` should evolve the target schema under `merge-schema=true`.
  test("merge into: explicit UPDATE SET newCol = s.x evolves target schema (mergeSchema=true)") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (id INT, name STRING)
              | USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
        sql("INSERT INTO t VALUES (1, 'a')")

        spark
          .sql("SELECT 1 AS id, 'A' AS name, 99 AS score")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET t.id = s.id, t.name = s.name, score = s.score
              |""".stripMargin)

        checkAnswer(
          sql("SELECT id, name, score FROM t"),
          Seq(Row(1, "A", 99))
        )
      }
    }
  }

  // Scoped evolution: explicit INSERT (newCol1) must only add newCol1, even if the source has
  // additional unreferenced new columns. Only `*` actions pull in the full source schema.
  test("merge into: explicit INSERT (newCol1) scopes evolution to mentioned columns") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""CREATE TABLE t (id INT, name STRING)
              | USING paimon
              | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

        spark
          .sql("SELECT 2 AS id, 'b' AS name, 10 AS new_col1, 20 AS new_col2")
          .createOrReplaceTempView("s")

        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN NOT MATCHED THEN INSERT (id, name, new_col1) VALUES (s.id, s.name, s.new_col1)
              |""".stripMargin)

        val cols = spark.table("t").schema.fieldNames.toSet
        assert(cols == Set("id", "name", "new_col1"), s"unexpected columns: $cols")
        checkAnswer(sql("SELECT id, name, new_col1 FROM t"), Seq(Row(2, "b", 10)))
      }
    }
  }

  // Strict default: explicit assignment to a non-existent target column still fails — only
  // merge-schema mode treats it as a new-column intent.
  test("merge into: explicit INSERT (newCol) without merge-schema fails (mergeSchema=false)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)

      spark
        .sql("SELECT 2 AS id, 'b' AS name, 99 AS score")
        .createOrReplaceTempView("s")

      val ex = intercept[Exception] {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)
              |""".stripMargin)
      }
      assert(ex.getMessage.toLowerCase.contains("score"))
    }
  }

  // Explicit nested-field assignment via `t.s.c1 = ...`. The other struct field must be preserved
  // (handled by `applyFieldAssignments` recursing through the target struct).
  test("merge into: UPDATE explicit nested-field assignment preserves sibling field") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  id INT,
            |  info STRUCT<a: STRING, b: STRING>
            |) USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA', 'b', 'oldB'))")

      spark.sql("SELECT 1 AS id, 'newA' AS new_a").createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET t.info.a = s.new_a""".stripMargin)

      checkAnswer(sql("SELECT id, info.a, info.b FROM t"), Seq(Row(1, "newA", "oldB")))
    }
  }

  test("merge into: top-level column names match case-insensitively (UPDATE * / INSERT *)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      // Source uses uppercase, target uses lowercase.
      spark
        .sql("SELECT 1 AS ID, 'A' AS NAME UNION ALL SELECT 3 AS ID, 'c' AS NAME")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.ID
            | WHEN MATCHED THEN UPDATE SET *
            | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(1, "A"), Row(2, "b"), Row(3, "c"))
      )
    }
  }

  test("merge into: explicit SET target column LHS matches case-insensitively") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'old')")

      spark.sql("SELECT 1 AS id, 'NEW' AS new_name").createOrReplaceTempView("s")

      // LHS `NAME` (uppercase) matches target `name`.
      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET NAME = s.new_name""".stripMargin)

      checkAnswer(sql("SELECT id, name FROM t"), Seq(Row(1, "NEW")))
    }
  }

  test("merge into: nested struct field names match case-insensitively (UPDATE *)") {
    withTable("t") {
      sql("""CREATE TABLE t (
            |  id INT,
            |  info STRUCT<a: STRING, b: STRING>
            |) USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('a', 'oldA', 'b', 'oldB'))")

      // Source struct fields use uppercase.
      spark
        .sql("SELECT 1 AS id, named_struct('A', 'newA', 'B', 'newB') AS info")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

      checkAnswer(sql("SELECT id, info.a, info.b FROM t"), Seq(Row(1, "newA", "newB")))
    }
  }
}

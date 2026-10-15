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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.ExceptionUtils

import org.apache.spark.sql.Row

import java.util.concurrent.ThreadLocalRandom

class CopyFilesProcedureTest extends PaimonSparkTestBase {

  test("Paimon copy files procedure: append table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      sql(s"INSERT INTO tbl$random VALUES (4, 'd'), (5, 'e'), (6, 'f')")

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )
    }
  }

  test("Paimon copy files procedure: partitioned append table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )
    }
  }

  test("Paimon copy files procedure: partitioned append table with partition filter") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      checkAnswer(
        sql(
          s"""CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random', where => "dt = '2025-08-17' and hh = 5")"""),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random WHERE dt = '2025-08-17' and hh = 5")
      )
    }
  }

  test("Paimon copy files procedure: pk table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |TBLPROPERTIES (
             |  'primary-key' = 'dt,hh,k',
             |  'bucket' = '-1')
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

    }
  }

  test("Paimon copy files procedure: schema change") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      // source table
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)
      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")

      sql(s"""
             |ALTER TABLE tbl$random
             |DROP COLUMN v
             |""".stripMargin)

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

    }
  }

  test("Paimon copy files procedure: copy to existed table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      // source table
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)
      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")

      // target table
      sql(s"""
             |CREATE TABLE target_tbl$random (k INT, v STRING, dt STRING, hh INT, v2 STRING)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)
      // partition should overwrite
      sql(
        s"INSERT INTO target_tbl$random VALUES (3, 'c', '2025-08-17', 5, 'c1'), (4, 'd', '2025-08-17', 6, 'd1')")

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random WHERE dt = '2025-08-17' and hh = 5"),
        Row(1, "a", "2025-08-17", 5, null)
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random WHERE dt = '2025-10-06' and hh = 0"),
        Row(2, "b", "2025-10-06", 0, null)
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random WHERE dt = '2025-08-17' and hh = 6"),
        Row(4, "d", "2025-08-17", 6, "d1")
      )
    }
  }

  test("Paimon copy files procedure: copy to existed compatible table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      // source table
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      // target table
      sql(s"""
             |CREATE TABLE target_tbl$random (k INT, v2 STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      assertThrows[RuntimeException] {
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')")
      }
    }
  }

  test("Paimon copy files procedure: rows written after the copy get new row ids") {
    withTable("src", "dst") {
      sql("CREATE TABLE src (id INT, v STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO src VALUES (1, 'a')")
      sql("INSERT INTO src VALUES (2, 'b')")

      checkAnswer(
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')"),
        Row(true) :: Nil)
      // the copied rows keep their row ids
      checkAnswer(sql("SELECT id, _ROW_ID FROM dst"), Seq(Row(1, 0), Row(2, 1)))

      // rows written afterwards continue after them instead of reusing them
      sql("INSERT INTO dst VALUES (3, 'c'), (4, 'd')")
      checkAnswer(
        sql("SELECT id, _ROW_ID FROM dst"),
        Seq(Row(1, 0), Row(2, 1), Row(3, 2), Row(4, 3)))
    }
  }

  test("Paimon copy files procedure: copied rows follow the row ids of rows the copy keeps") {
    withTable("src", "dst") {
      sql("""
            |CREATE TABLE src (id INT, v STRING, pt STRING) PARTITIONED BY (pt)
            |TBLPROPERTIES ('row-tracking.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO src VALUES (1, 'a', 'p1'), (2, 'b', 'p1')")
      sql("""
            |CREATE TABLE dst (id INT, v STRING, pt STRING) PARTITIONED BY (pt)
            |TBLPROPERTIES ('row-tracking.enabled' = 'true')
            |""".stripMargin)
      sql("INSERT INTO dst VALUES (10, 'x', 'p2'), (11, 'y', 'p2')")

      // only partition p1 is overwritten: the rows of p2 keep row ids 0 and 1
      checkAnswer(
        sql(
          """CALL sys.copy(source_table => 'src', target_table => 'dst', where => "pt = 'p1'")"""),
        Row(true) :: Nil)
      checkAnswer(
        sql("SELECT id, v, _ROW_ID FROM dst"),
        Seq(Row(1, "a", 2), Row(2, "b", 3), Row(10, "x", 0), Row(11, "y", 1)))

      sql("INSERT INTO dst VALUES (3, 'c', 'p1')")
      checkAnswer(sql("SELECT _ROW_ID FROM dst WHERE id = 3"), Row(4) :: Nil)
      checkAnswer(sql("SELECT count(DISTINCT _ROW_ID), count(*) FROM dst"), Row(5, 5) :: Nil)
    }
  }

  test("Paimon copy files procedure: updates after copying a data-evolution table take effect") {
    withTable("src", "dst") {
      sql("""
            |CREATE TABLE src (id INT, v STRING) TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'compaction.min.file-num' = '2')
            |""".stripMargin)
      // many snapshots: the source files carry sequence numbers above the snapshots of dst
      (1 to 10).foreach(i => sql(s"INSERT INTO src VALUES ($i, 'v$i')"))
      sql("UPDATE src SET v = 'u1' WHERE id = 1")

      // the update is a file holding the same rows as another one, which cannot be copied
      val error = intercept[Exception] {
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')")
      }
      val trace = ExceptionUtils.stringifyException(error)
      assert(trace.contains("Compact the source table first"), trace)
      checkAnswer(sql("SELECT count(*) FROM dst"), Row(0) :: Nil)

      sql("CALL sys.compact(table => 'src')")
      checkAnswer(
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')"),
        Row(true) :: Nil)
      checkAnswer(sql("SELECT id, v FROM dst"), sql("SELECT id, v FROM src"))
      checkAnswer(sql("SELECT v FROM dst WHERE id = 1"), Row("u1") :: Nil)
      checkAnswer(sql("SELECT count(DISTINCT _ROW_ID), count(*) FROM dst"), Row(10, 10) :: Nil)

      sql("UPDATE dst SET v = 'u2' WHERE id = 2")
      sql("""
            |MERGE INTO dst USING (SELECT _ROW_ID AS rid FROM dst WHERE id IN (1, 10)) s
            |ON dst._ROW_ID = s.rid
            |WHEN MATCHED THEN UPDATE SET v = 'm'
            |""".stripMargin)
      checkAnswer(
        sql("SELECT id, v FROM dst WHERE id IN (1, 2, 3, 10) ORDER BY id"),
        Seq(Row(1, "m"), Row(2, "u2"), Row(3, "v3"), Row(10, "m")))
    }
  }

  test("Paimon copy files procedure: files that store their row ids are refused") {
    withTable("src", "dst") {
      sql("CREATE TABLE src (id INT, v STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO src VALUES (1, 'a'), (2, 'b')")
      // a copy-on-write update stores the row ids of the rewritten rows in the new file
      sql("UPDATE src SET v = 'A' WHERE id = 1")
      assert(
        sql("SELECT write_cols FROM `src$files`")
          .collect()
          .exists(row => !row.isNullAt(0) && row.getSeq[String](0).contains("_ROW_ID")))

      val error = intercept[Exception] {
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')")
      }
      val trace = ExceptionUtils.stringifyException(error)
      assert(trace.contains("its rows store their row ids"), trace)
    }
  }

  test("Paimon copy files procedure: data-evolution table with a vector field") {
    withTable("src", "dst") {
      sql("""
            |CREATE TABLE src (id INT, embedding ARRAY<FLOAT>) TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'file.format' = 'parquet',
            |  'vector.file.format' = 'parquet',
            |  'vector-field' = 'embedding',
            |  'field.embedding.vector-dim' = '2')
            |""".stripMargin)
      sql("INSERT INTO src VALUES (1, array(1.0f, 0.0f)), (2, array(2.0f, 0.0f))")
      val vectorFiles = fileNames("src").count(_.contains(".vector."))
      assert(vectorFiles > 0, fileNames("src"))

      checkAnswer(
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')"),
        Row(true) :: Nil)
      // the vector file is copied as a vector file
      assert(fileNames("dst").count(_.contains(".vector.")) == vectorFiles, fileNames("dst"))
      checkAnswer(
        sql("SELECT id, embedding, _ROW_ID FROM dst"),
        sql("SELECT id, embedding, _ROW_ID FROM src"))

      sql("INSERT INTO dst VALUES (3, array(3.0f, 0.0f))")
      checkAnswer(
        sql("SELECT id, embedding, _ROW_ID FROM dst WHERE id = 3"),
        Row(3, Seq(3.0f, 0.0f), 2) :: Nil)
    }
  }

  test("Paimon copy files procedure: data-evolution table with a blob field") {
    withTable("src", "dst") {
      sql("""
            |CREATE TABLE src (id INT, picture BINARY) TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'blob-field' = 'picture')
            |""".stripMargin)
      sql("INSERT INTO src VALUES (1, CAST('p1' AS BINARY)), (2, CAST('p2' AS BINARY))")
      val blobFiles = fileNames("src").count(_.endsWith(".blob"))
      assert(blobFiles > 0, fileNames("src"))

      checkAnswer(
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')"),
        Row(true) :: Nil)
      // the blob file is copied as a blob file
      assert(fileNames("dst").count(_.endsWith(".blob")) == blobFiles, fileNames("dst"))
      checkAnswer(
        sql("SELECT id, CAST(picture AS STRING), _ROW_ID FROM dst"),
        sql("SELECT id, CAST(picture AS STRING), _ROW_ID FROM src"))
      checkAnswer(sql("SELECT CAST(picture AS STRING) FROM dst WHERE id = 2"), Row("p2") :: Nil)
    }
  }

  test("Paimon copy files procedure: partitioned data-evolution table with a vector field") {
    withTable("src", "dst") {
      sql("""
            |CREATE TABLE src (id INT, embedding ARRAY<FLOAT>, pt STRING)
            |PARTITIONED BY (pt) TBLPROPERTIES (
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'file.format' = 'parquet',
            |  'vector.file.format' = 'parquet',
            |  'vector-field' = 'embedding',
            |  'field.embedding.vector-dim' = '2')
            |""".stripMargin)
      sql("""
            |INSERT INTO src VALUES
            |(1, array(1.0f, 0.0f), 'a'), (2, array(2.0f, 0.0f), 'b'), (3, array(3.0f, 0.0f), 'c')
            |""".stripMargin)
      sql("INSERT INTO src VALUES (4, array(4.0f, 0.0f), 'a'), (5, array(5.0f, 0.0f), 'c')")

      // the commit may order the partitions in any way
      checkAnswer(
        sql("CALL sys.copy(source_table => 'src', target_table => 'dst')"),
        Row(true) :: Nil)
      checkAnswer(sql("SELECT * FROM dst"), sql("SELECT * FROM src"))
      checkAnswer(sql("SELECT count(DISTINCT _ROW_ID) = count(*) FROM dst"), Row(true) :: Nil)

      // only partition b is overwritten
      sql("INSERT INTO src VALUES (6, array(6.0f, 0.0f), 'b')")
      checkAnswer(
        sql("""CALL sys.copy(source_table => 'src', target_table => 'dst', where => "pt = 'b'")"""),
        Row(true) :: Nil)
      checkAnswer(sql("SELECT * FROM dst"), sql("SELECT * FROM src"))
      checkAnswer(sql("SELECT count(DISTINCT _ROW_ID) = count(*) FROM dst"), Row(true) :: Nil)
    }
  }

  private def fileNames(table: String): Seq[String] =
    sql(s"SELECT file_path FROM `$table$$files`").collect().map(_.getString(0)).toSeq
}

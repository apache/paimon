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

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.analysis.Delete

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}

class DeleteFromTableTest extends PaimonSparkTestBase {

  test(s"test delete from append only table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22')")

    assertThatThrownBy(() => spark.sql("DELETE FROM T WHERE name = 'a'"))
      .isInstanceOf(classOf[UnsupportedOperationException])
  }

  CoreOptions.MergeEngine.values().foreach {
    mergeEngine =>
      {
        test(s"test delete with merge engine $mergeEngine") {
          val options = if ("first-row".equals(mergeEngine.toString)) {
            s"'primary-key' = 'id', 'merge-engine' = '$mergeEngine', 'changelog-producer' = 'lookup'"
          } else {
            s"'primary-key' = 'id', 'merge-engine' = '$mergeEngine'"
          }
          spark.sql(s"""
                       |CREATE TABLE T (id INT, name STRING, dt STRING)
                       |TBLPROPERTIES ($options)
                       |""".stripMargin)

          spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22')")

          if (Delete.supportedMergeEngine.contains(mergeEngine)) {
            spark.sql("DELETE FROM T WHERE name = 'a'")
          } else
            assertThatThrownBy(() => spark.sql("DELETE FROM T WHERE name = 'a'"))
              .isInstanceOf(classOf[UnsupportedOperationException])
        }
      }
  }

  test(s"test delete with primary key") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, name STRING, dt STRING)
         |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'merge-engine' = 'deduplicate')
         |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("DELETE FROM T WHERE id = 1")

    val rows1 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[2,b,22], [3,c,33]]")

    spark.sql("DELETE FROM T WHERE id < 3")

    val rows2 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete with non-primary key") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, name STRING, dt STRING)
         |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'merge-engine' = 'deduplicate')
         |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33'), (4, 'a', '44')")

    spark.sql("DELETE FROM T WHERE name = 'a'")

    val rows1 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[2,b,22], [3,c,33]]")

    spark.sql("DELETE FROM T WHERE name < 'c'")

    val rows2 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete with no where") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("DELETE FROM T")

    val rows = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows.toString).isEqualTo("[]")
  }

  test(s"test delete with in condition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("DELETE FROM T WHERE id IN (1, 2)")

    val rows = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete with in subquery") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    import testImplicits._
    val df = Seq(1, 2).toDF("id")
    df.createOrReplaceTempView("deleted_ids")
    spark.sql("DELETE FROM T WHERE id IN (SELECT * FROM deleted_ids)")

    val rows = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete is drop partition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt, hh', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES " +
        "(1, 'a', '2023-10-01', '12')," +
        "(2, 'b', '2023-10-01', '12')," +
        "(3, 'c', '2023-10-02', '12')," +
        "(4, 'd', '2023-10-02', '13')," +
        "(5, 'e', '2023-10-02', '14')," +
        "(6, 'f', '2023-10-02', '15')")

    // delete isn't drop partition
    spark.sql("DELETE FROM T WHERE name = 'a' and hh = '12'")
    val rows1 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows1.toString).isEqualTo(
      "[[2,b,2023-10-01,12], [3,c,2023-10-02,12], [4,d,2023-10-02,13], [5,e,2023-10-02,14], [6,f,2023-10-02,15]]")

    // delete is drop partition
    spark.sql("DELETE FROM T WHERE hh = '12'")
    val rows2 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows2.toString).isEqualTo(
      "[[4,d,2023-10-02,13], [5,e,2023-10-02,14], [6,f,2023-10-02,15]]")

    spark.sql("DELETE FROM T WHERE dt = '2023-10-02' and hh = '13'")
    val rows3 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows3.toString).isEqualTo("[[5,e,2023-10-02,14], [6,f,2023-10-02,15]]")

    spark.sql("DELETE FROM T WHERE dt = '2023-10-02'")
    val rows4 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows4.toString).isEqualTo("[]")
  }
}

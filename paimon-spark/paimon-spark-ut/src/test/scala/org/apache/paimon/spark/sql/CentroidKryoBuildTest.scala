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

import org.apache.paimon.data.{BinaryString, GenericArray, GenericRow}
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.SparkConf

class CentroidKryoBuildTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")

  test("centroid sampling and shuffle work with strict Kryo registration") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (v ARRAY<FLOAT>, tag INT, rank INT) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                  |'global-index.row-count-per-shard'='8')""".stripMargin)
      val table = loadTable("T")
      val builder = table.newBatchWriteBuilder()
      val write = builder.newWrite()
      val commit = builder.newCommit()
      try {
        (0 until 32).foreach(
          i =>
            write.write(
              GenericRow.of(new GenericArray(Array(i.toFloat, 1f)), Int.box(i), Int.box(32 - i))))
        commit.commit(write.prepareCommit())
      } finally {
        write.close()
        commit.close()
      }
      spark.sql(
        """CALL sys.create_global_index(table=>'test.T',index_column=>'v',
          |index_type=>'ivf-flat',
          |options=>'ivf-flat.sharding=centroid,ivf-flat.dimension=2,ivf-flat.metric=l2,ivf-flat.centroid.nlist=2')""".stripMargin)
      val result = table
        .newVectorSearchBuilder()
        .withVectorColumn("v")
        .withVector(Array(7f, 1f))
        .withLimit(1)
        .executeLocal()
      assert(result.results().toArray(1).toSeq == Seq(7L))
      spark.sql(
        """CALL sys.create_global_index(table=>'test.T',index_column=>'v',
          |index_type=>'ivf-flat',
          |extra_fields=>'{"tag":"bitmap"}',
          |options=>'ivf-flat.sharding=centroid,ivf-flat.dimension=2,ivf-flat.metric=l2,ivf-flat.centroid.nlist=2')""".stripMargin)
      val filtered = table
        .newVectorSearchBuilder()
        .withVectorColumn("v")
        .withVector(Array(0f, 1f))
        .withLimit(1)
        .withFilter(new PredicateBuilder(table.rowType()).equal(1, Int.box(7)))
        .executeLocal()
      assert(filtered.results().toArray(1).toSeq == Seq(7L))
      spark.sql("""CALL sys.add_global_index_fields(table=>'test.T', index_column=>'v',
                  |index_type=>'ivf-flat', extra_fields=>'{"rank":"bitmap"}')""".stripMargin)
      val appended = table
        .newVectorSearchBuilder()
        .withVectorColumn("v")
        .withVector(Array(0f, 1f))
        .withLimit(1)
        .withFilter(new PredicateBuilder(table.rowType()).equal(2, Int.box(25)))
        .executeLocal()
      assert(appended.results().toArray(1).toSeq == Seq(7L))
    }
  }

  test("route appended fields for ranges with different existing Bitmap fields") {
    withTable("T") {
      spark.sql(
        """CREATE TABLE T (id INT, v ARRAY<FLOAT>, tenant INT, status STRING, priority INT, p INT)
          |PARTITIONED BY (p) TBLPROPERTIES (
          |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
          |'vector-index.search-mode'='fast', 'vector-index.post-filter.candidates-factor'='1')""".stripMargin)
      val builder = loadTable("T").newBatchWriteBuilder()
      val write = builder.newWrite()
      val commit = builder.newCommit()
      try {
        Seq(
          (10, 0f, 1, "pending", Int.box(0), 0),
          (11, 8f, 1, "ready", Int.box(7), 0),
          (20, 0f, 2, "pending", Int.box(0), 1),
          (21, 9f, 2, "ready", null, 1)).foreach {
          case (id, x, tenant, status, priority, partition) =>
            write.write(
              GenericRow.of(
                Int.box(id),
                new GenericArray(Array(x, 0f)),
                Int.box(tenant),
                BinaryString.fromString(status),
                priority,
                Int.box(partition)))
        }
        commit.commit(write.prepareCommit())
      } finally {
        write.close()
        commit.close()
      }

      def build(partition: Int, fields: String): Unit = {
        spark
          .sql(
            s"""CALL sys.create_global_index(table=>'test.T', index_column=>'v',
               |index_type=>'ivf-flat', extra_fields=>'$fields', partitions=>'p=$partition',
               |options=>'ivf-flat.sharding=centroid,ivf-flat.dimension=2,ivf-flat.metric=l2,ivf-flat.centroid.nlist=1')""".stripMargin)
          .collect()
      }
      build(0, """{"tenant":"bitmap"}""")
      build(1, """{"tenant":"bitmap","status":"bitmap"}""")
      assert(
        spark
          .sql("""CALL sys.add_global_index_fields(table=>'test.T', index_column=>'v', index_type=>'ivf-flat',
                 |extra_fields=>'{"status":"bitmap","priority":"bitmap"}')""".stripMargin)
          .head()
          .getBoolean(0))

      def nearest(partition: Int, predicate: String): Seq[Int] =
        spark
          .sql(s"""SELECT id FROM vector_search('T', 'v', array(0.0f, 0.0f), 1)
                  |WHERE p = $partition AND $predicate""".stripMargin)
          .collect()
          .map(_.getInt(0))
          .toSeq

      assert(nearest(0, "status = 'ready' AND priority = 7") == Seq(11))
      assert(nearest(1, "status = 'ready' AND priority IS NULL") == Seq(21))
    }
  }
}

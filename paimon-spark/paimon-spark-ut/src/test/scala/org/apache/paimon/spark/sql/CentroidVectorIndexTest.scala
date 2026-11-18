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

import org.apache.paimon.globalindex.{VectorIndexShardMeta, VectorTrainingModelIO}
import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.spark.PaimonSparkTestBase

import scala.collection.JavaConverters._

/** End-to-end centroid build over nonzero, disjoint source row-ID ranges. */
class CentroidVectorIndexTest extends PaimonSparkTestBase {

  private def build(): Unit = {
    spark
      .sql("""CALL sys.create_global_index(
             |table => 'test.T', index_column => 'v', index_type => 'ivf-pq',
             |partitions => 'p=0',
             |options => 'ivf-pq.shard=centroid-based,ivf-pq.nlist=4,ivf-pq.pq.m=1')""".stripMargin)
      .collect()
  }

  private def nearest(id: Int): Int = {
    spark
      .sql(s"""SELECT id FROM vector_search(
              |'T', 'v', array($id.0f, ${id % 17}.0f), 1,
              |map('ivf.nprobe', '1'))""".stripMargin)
      .collect()
      .head
      .getInt(0)
  }

  test("centroid build routes absolute IDs, preserves raw coverage and atomically rebuilds") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>, p INT) PARTITIONED BY (p)
                  |TBLPROPERTIES (
                  |'bucket' = '-1',
                  |'row-tracking.enabled' = 'true',
                  |'data-evolution.enabled' = 'true',
                  |'deletion-vectors.enabled' = 'true',
                  |'vector-index.search-mode' = 'detail',
                  |'ivf-pq.dimension' = '2',
                  |'ivf-pq.metric' = 'l2',
                  |'global-index.row-count-per-shard' = '64',
                  |'global-index.build.max-parallelism' = '2')""".stripMargin)
      def insert(from: Int, to: Int, partition: Int): Unit = {
        spark.sql(s"""INSERT INTO T SELECT cast(id AS INT),
                     |array(cast(id AS FLOAT), cast(id % 17 AS FLOAT)), $partition
                     |FROM range($from, $to)""".stripMargin)
      }
      insert(-64, 0, 2)
      insert(0, 128, 0)
      insert(128, 256, 1)
      insert(256, 384, 0)
      build()
      val table = loadTable("T")
      def indexes: Seq[IndexFileMeta] =
        table
          .store()
          .newIndexFileHandler()
          .scan(table.snapshotManager().latestSnapshot(), "ivf-pq")
          .asScala
          .map(_.indexFile())
          .toSeq

      val firstFiles = indexes.map(_.fileName()).toSet
      assert(indexes.size == 5)
      assert(indexes.count(_.isRoutingModel()) == 1)
      assert(nearest(300) == 300)
      // Partition 1 lies inside the indexed partition's min/max envelope, but is unindexed.
      assert(nearest(200) == 200)

      build()
      assert(indexes.size == 5)
      assert(indexes.map(_.fileName()).toSet.intersect(firstFiles).isEmpty)
      assert(nearest(300) == 300)
      assert(nearest(200) == 200)

      intercept[Exception] {
        spark
          .sql(
            """CALL sys.create_global_index(table => 'test.T', index_column => 'v',
              |index_type => 'ivf-pq',
              |options => 'ivf-pq.shard=centroid-based,ivf-pq.nlist=4,ivf-pq.pq.m=1')""".stripMargin)
          .collect()
      }
      assert(indexes.size == 5)

      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'btree')")
        .collect()
      val filtered = spark
        .sql("""SELECT id FROM vector_search('T', 'v', array(300.0f, 11.0f), 1,
               |map('ivf.nprobe', '4')) WHERE id = 300""".stripMargin)
        .collect()
      assert(filtered.map(_.getInt(0)).toSeq == Seq(300))
      spark.sql("DELETE FROM T WHERE p = 0 AND id = 300")
      assert(nearest(300) != 300)
    }
  }
  test("distributed training preserves the model across sparse splits and different task counts") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                  |'deletion-vectors.enabled'='true', 'vector-index.search-mode'='fast',
                  |'ivf-flat.dimension'='2', 'ivf-flat.metric'='l2',
                  |'global-index.row-count-per-shard'='32',
                  |'global-index.build.max-parallelism'='4')""".stripMargin)
      spark.sql("""INSERT INTO T SELECT cast(id AS INT),
                  |CASE WHEN id = 4 OR id >= 192 THEN array(cast(id AS FLOAT), 1.0f)
                  |ELSE cast(NULL AS ARRAY<FLOAT>) END FROM range(256)""".stripMargin)
      spark.sql("DELETE FROM T WHERE id = 200")
      def buildModel(): Array[Byte] = {
        spark.sql(
          """CALL sys.create_global_index(table => 'test.T', index_column => 'v',
            |index_type => 'ivf-flat',
            |options => 'ivf-flat.shard=centroid-based,ivf-flat.nlist=2,ivf-flat.train.sample-ratio=0.125')""".stripMargin)
        val table = loadTable("T")
        val model = table
          .store()
          .newIndexFileHandler()
          .scan(table.snapshotManager().latestSnapshot(), "ivf-flat")
          .asScala
          .map(_.indexFile())
          .find(_.isRoutingModel())
          .get
        val meta = VectorIndexShardMeta.deserialize(model.globalIndexMeta().indexMeta())
        val path = table.store().pathFactory().globalIndexFileFactory().toPath(model)
        val in = table.fileIO().newInputStream(path)
        try VectorTrainingModelIO.read(in, model.fileSize(), meta.modelId())
        finally in.close()
      }
      val fourTasks = buildModel()
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('global-index.build.max-parallelism'='2')")
      assert(fourTasks.sameElements(buildModel()))
      def nearest(id: Int): Int =
        spark
          .sql(s"""SELECT id FROM vector_search('T','v',array($id.0f,1.0f),1,
                  |map('centroid.nprobe','2'))""".stripMargin)
          .collect()
          .head
          .getInt(0)
      assert(nearest(4) == 4)
      assert(nearest(255) == 255)
      assert(nearest(200) != 200)
    }
  }

  Seq("ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann").foreach {
    kind =>
      test(s"$kind centroid SQL build, read, filter and range rebuild") {
        withTable("T") {
          spark.sql(s"""CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                       |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                       |'data-evolution.enabled'='true', 'deletion-vectors.enabled'='true',
                       |'vector-index.search-mode'='fast', '$kind.dimension'='4',
                       |'$kind.metric'='l2', 'global-index.row-count-per-shard'='64',
                       |'global-index.build.max-parallelism'='2')""".stripMargin)
          spark.sql(
            """INSERT INTO T SELECT cast(id AS INT),
              |array(cast(id AS FLOAT), cast(id % 17 AS FLOAT),
              |cast(id % 23 AS FLOAT), cast(id % 31 AS FLOAT)) FROM range(0, 256)""".stripMargin)
          val pq = if (kind == "ivf-pq" || kind == "diskann") s",$kind.pq.m=1" else ""
          def create(mode: String): Unit =
            spark
              .sql(s"""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
                      |index_type => '$kind',
                      |options => '$kind.shard=$mode,$kind.nlist=4$pq')""".stripMargin)
              .collect()
          def nearest(mode: String): Array[Int] = {
            val opts =
              if (kind == "diskann" && mode == "centroid-based")
                "map('centroid.nprobe','4','diskann.l_search','256')"
              else if (kind == "diskann") "map('diskann.l_search','256')"
              else if (mode == "centroid-based") "map('centroid.nprobe','2','ivf.nprobe','3')"
              else "map('ivf.nprobe','4')"
            spark
              .sql(s"""SELECT id FROM vector_search('T','v',array(200.0f,13.0f,16.0f,14.0f),1,
                      |$opts) WHERE id >= 192""".stripMargin)
              .collect()
              .map(_.getInt(0))
          }
          create("centroid-based")
          val table = loadTable("T")
          val files = table
            .store()
            .newIndexFileHandler()
            .scan(table.snapshotManager().latestSnapshot(), kind)
            .asScala
            .map(_.indexFile())
          assert(files.size == 5 && files.count(_.isRoutingModel()) == 1)
          assert(nearest("centroid-based").toSeq == Seq(200))
          val centroids = files
            .filterNot(_.isRoutingModel())
            .map(f => VectorIndexShardMeta.deserialize(f.globalIndexMeta().indexMeta()).centroid())
          assert(centroids.toSet == Set(0, 1, 2, 3))
          spark
            .sql(s"""CALL sys.drop_global_index(table => 'test.T', index_column => 'v',
                    |index_type => '$kind')""".stripMargin)
            .collect()
          create("range-based")
          assert(nearest("range-based").toSeq == Seq(200))
        }
      }
  }

}

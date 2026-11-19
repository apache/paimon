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

import org.apache.paimon.globalindex.CentroidVectorIndexUtils
import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.vector.index.{VectorIndexShardMeta, VectorTrainingModelIO}

import scala.collection.JavaConverters._

/** End-to-end centroid build over nonzero, disjoint source row-ID ranges. */
class CentroidVectorIndexTest extends PaimonSparkTestBase {

  private def build(): Unit = {
    spark
      .sql("""CALL sys.create_global_index(
             |table => 'test.T', index_column => 'v', index_type => 'ivf-pq-centroid',
             |partitions => 'p=0',
             |options => 'ivf-pq-centroid.nlist=4,ivf-pq-centroid.pq.m=1')""".stripMargin)
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
                  |'ivf-pq-centroid.dimension' = '2',
                  |'ivf-pq-centroid.metric' = 'l2',
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
          .scan(table.snapshotManager().latestSnapshot(), "ivf-pq-centroid")
          .asScala
          .map(_.indexFile())
          .toSeq

      val firstFiles = indexes.map(_.fileName()).toSet
      assert(indexes.size == 5)
      assert(indexes.count(f => CentroidVectorIndexUtils.isRoutingModel(f)) == 1)
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
          .sql("""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
                 |index_type => 'ivf-pq-centroid',
                 |options => 'ivf-pq-centroid.nlist=4,ivf-pq-centroid.pq.m=1')""".stripMargin)
          .collect()
      }
      assert(indexes.size == 5)

      def filteredIds(): Seq[Int] =
        spark
          .sql("""SELECT id FROM vector_search('T', 'v', array(300.0f, 11.0f), 1,
                 |map('ivf.nprobe', '4')) WHERE id = 300""".stripMargin)
          .collect()
          .map(_.getInt(0))
          .toSeq

      // Post-filtering before the scalar index exists must preserve absolute IDs.
      assert(filteredIds() == Seq(300))
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'btree')")
        .collect()
      assert(filteredIds() == Seq(300))
      spark.sql("DELETE FROM T WHERE p = 0 AND id = 300")
      assert(nearest(300) != 300)
    }
  }
  Seq(true, false).foreach {
    centroidFirst =>
      test(
        s"range and centroid partitions share vector search with centroid built first=$centroidFirst") {
        withTable("T") {
          spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>, p INT) PARTITIONED BY (p)
                      |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                      |'data-evolution.enabled'='true', 'vector-index.search-mode'='detail',
                      |'fields.v.dimension'='2', 'fields.v.metric'='l2',
                      |'global-index.row-count-per-shard'='32')""".stripMargin)
          Seq(0, 1, 2).foreach {
            part =>
              val first = if (part == 2) 5000 else 0
              spark.sql(s"""INSERT INTO T SELECT CAST(id + ${part * 128} AS INT),
                           |array(CAST(IF(id = 0, $first, 1000 + id) AS FLOAT), 0.0f), $part
                           |FROM range(0, 128, 1, 1)""".stripMargin)
          }
          val builds = Seq(("ivf-pq-centroid", 0), ("ivf-pq", 1))
          (if (centroidFirst) builds else builds.reverse).foreach {
            case (kind, part) =>
              spark
                .sql(s"""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
                        |index_type => '$kind', partitions => 'p=$part',
                        |options => '$kind.nlist=2,$kind.pq.m=1')""".stripMargin)
                .collect()
          }

          val ids = spark
            .sql("""SELECT id FROM vector_search('T','v',array(0.0f,0.0f),2,
                   |map('ivf.nprobe','2','centroid.nprobe','2'))""".stripMargin)
            .collect()
            .map(_.getInt(0))
            .toSet
          assert(ids == Set(0, 128))

          val table = loadTable("T")
          val local = table
            .newVectorSearchBuilder()
            .withVectorColumn("v")
            .withVector(Array(0.0f, 0.0f))
            .withLimit(2)
            .withOption("centroid.nprobe", "2")
            .withOption("ivf.nprobe", "2")
            .executeLocal()
          assert(local.results().toArray(2).toSet == Set(0L, 128L))
          val batch = table
            .newBatchVectorSearchBuilder()
            .withVectorColumn("v")
            .withVectors(Array(Array(0.0f, 0.0f), Array(5000.0f, 0.0f)))
            .withLimit(2)
            .withOption("centroid.nprobe", "2")
            .withOption("ivf.nprobe", "2")
            .executeBatchLocal()
          assert(batch.get(0).results().toArray(2).toSet == Set(0L, 128L))
          assert(batch.get(1).results().contains(256L))
        }
      }
  }

  test("distributed training preserves the model across sparse splits and different task counts") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                  |'deletion-vectors.enabled'='true', 'vector-index.search-mode'='fast',
                  |'ivf-flat-centroid.dimension'='2', 'ivf-flat-centroid.metric'='l2',
                  |'global-index.row-count-per-shard'='32',
                  |'global-index.build.max-parallelism'='4')""".stripMargin)
      spark.sql("""INSERT INTO T SELECT cast(id AS INT),
                  |CASE WHEN id = 4 OR id >= 192 THEN array(cast(id AS FLOAT), 1.0f)
                  |ELSE cast(NULL AS ARRAY<FLOAT>) END FROM range(256)""".stripMargin)
      spark.sql("DELETE FROM T WHERE id = 200")
      def buildModel(): Array[Byte] = {
        spark.sql(
          """CALL sys.create_global_index(table => 'test.T', index_column => 'v',
            |index_type => 'ivf-flat-centroid',
            |options => 'ivf-flat-centroid.nlist=2,ivf-flat-centroid.train.sample-ratio=0.125')""".stripMargin)
        val table = loadTable("T")
        val model = table
          .store()
          .newIndexFileHandler()
          .scan(table.snapshotManager().latestSnapshot(), "ivf-flat-centroid")
          .asScala
          .map(_.indexFile())
          .find(f => CentroidVectorIndexUtils.isRoutingModel(f))
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
                       |'vector-index.search-mode'='fast', 'fields.v.dimension'='4',
                       |'fields.v.metric'='l2', 'global-index.row-count-per-shard'='64',
                       |'global-index.build.max-parallelism'='2')""".stripMargin)
          spark.sql(
            """INSERT INTO T SELECT cast(id AS INT),
              |array(cast(id AS FLOAT), cast(id % 17 AS FLOAT),
              |cast(id % 23 AS FLOAT), cast(id % 31 AS FLOAT)) FROM range(0, 256)""".stripMargin)
          val centroidType = s"$kind-centroid"
          def create(indexType: String): Unit = {
            val pq = if (kind == "ivf-pq" || kind == "diskann") s",$indexType.pq.m=1" else ""
            spark
              .sql(
                s"""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
                   |index_type => '$indexType', options => '$indexType.nlist=4$pq')""".stripMargin)
              .collect()
          }
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
          create(centroidType)
          val table = loadTable("T")
          val files = table
            .store()
            .newIndexFileHandler()
            .scan(table.snapshotManager().latestSnapshot(), centroidType)
            .asScala
            .map(_.indexFile())
          assert(
            files.size == 5 && files.count(f => CentroidVectorIndexUtils.isRoutingModel(f)) == 1)
          assert(nearest("centroid-based").toSeq == Seq(200))
          val centroids = files
            .filterNot(f => CentroidVectorIndexUtils.isRoutingModel(f))
            .map(f => VectorIndexShardMeta.deserialize(f.globalIndexMeta().indexMeta()).centroid())
          assert(centroids.toSet == Set(0, 1, 2, 3))
          spark
            .sql(s"""CALL sys.drop_global_index(table => 'test.T', index_column => 'v',
                    |index_type => '$centroidType')""".stripMargin)
            .collect()
          create(kind)
          assert(nearest("range-based").toSeq == Seq(200))
        }
      }
  }

}

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

import org.apache.paimon.append.dataevolution.DataEvolutionRowIdReassigner
import org.apache.paimon.globalindex.{CentroidIndexFile, CentroidIndexMeta, DataEvolutionGlobalIndexScanner}
import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.source.RawVectorSearchSplit
import org.apache.paimon.vector.index.VectorTrainingModelIO

import scala.collection.JavaConverters._

/** End-to-end centroid build over nonzero, disjoint source row-ID ranges. */
class CentroidVectorIndexTest extends PaimonSparkTestBase {

  test("centroid bitmap fields pre-filter top K and follow the vector definition") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>, tenant STRING, status STRING)
                  |TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                  |'global-index.row-count-per-shard'='128',
                  |'vector-index.search-mode'='detail',
                  |'vector-index.post-filter.candidates-factor'='1')""".stripMargin)
      spark.sql("""INSERT INTO T SELECT cast(id AS INT),
                  |array(cast(IF(id < 32, id, id + 1000) AS FLOAT), 0.0f),
                  |IF(id % 2 = 0, 'red', 'blue'),
                  |CASE WHEN id = 48 THEN NULL ELSE 'busy' END FROM range(64)""".stripMargin)
      def build(extraFields: String = "", sharding: String = "centroid"): Unit = {
        val extra = if (extraFields.isEmpty) "" else s", extra_fields => '$extraFields'"
        spark
          .sql(
            s"""CALL sys.create_global_index(table=>'test.T',index_column=>'v',
               |index_type=>'ivf-flat', options=>'ivf-flat.sharding=$sharding,ivf-flat.dimension=2,
               |ivf-flat.metric=l2,ivf-flat.centroid.nlist=2,ivf-flat.nlist=1'$extra)""".stripMargin
              .replace("\n", ""))
          .collect()
      }
      def files: Seq[IndexFileMeta] = {
        val table = loadTable("T")
        table
          .store()
          .newIndexFileHandler()
          .scan(table.snapshotManager().latestSnapshot(), "ivf-flat")
          .asScala
          .map(_.indexFile())
          .toSeq
      }
      def nearest(predicate: String): Seq[Int] =
        spark
          .sql(s"""SELECT id FROM vector_search('T','v',array(0.0f,0.0f),1,
                  |map('centroid.nprobe','2','ivf.nprobe','1')) WHERE $predicate""".stripMargin)
          .collect()
          .map(_.getInt(0))
          .toSeq

      build()
      assert(nearest("status IS NULL").isEmpty)

      def assertBuildRejected(
          fields: String,
          message: String,
          sharding: String = "centroid"): Unit = {
        val previousFiles = files.map(_.fileName()).toSet
        val error = intercept[Exception](build(fields, sharding))
        assert(
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(e => Option(e.getMessage).exists(_.contains(message))))
        assert(files.map(_.fileName()).toSet == previousFiles)
      }
      assertBuildRejected("""{"status":"btree"}""", "Unsupported index type")
      assertBuildRejected("""{"status":"bitmap","status":"bitmap"}""", "Duplicate field")
      assertBuildRejected("""{"status":"bitmap"}""", "requires centroid sharding", "range")

      build("""{"tenant":"bitmap","status":"bitmap"}""")
      assert(files.count(CentroidIndexMeta.isRoutingModel) == 1)
      assert(files.count(CentroidIndexMeta.isScalarIndex) == 2)
      assert(files.filter(CentroidIndexMeta.isScalarIndex).map(_.rowCount()).sum == 64)
      val coveredPlan = loadTable("T")
        .newVectorSearchBuilder()
        .withVectorColumn("v")
        .withVector(Array(0f, 0f))
        .withLimit(1)
        .newVectorScan()
        .scan()
      assert(!coveredPlan.splits().asScala.exists(_.isInstanceOf[RawVectorSearchSplit]))
      assert(nearest("tenant IN ('red', 'green') AND status IS NULL") == Seq(48))
      assert(nearest("tenant = 'blue' AND status IS NULL").isEmpty)

      build("""{"status":"bitmap"}""")
      assert(files.count(CentroidIndexMeta.isRoutingModel) == 1)
      assert(files.forall(_.globalIndexMeta().extraFieldIds().length == 1))
      assert(nearest("status IS NULL") == Seq(48))
      val table = loadTable("T")
      val statusIsNull = new PredicateBuilder(table.rowType())
        .isNull(table.rowType().getFieldIndex("status"))
      assert(!DataEvolutionGlobalIndexScanner.create(table, null, statusIsNull).isPresent)
      assert(
        spark.sql("SELECT id FROM T WHERE status IS NULL").collect().map(_.getInt(0)).toSeq ==
          Seq(48))

      intercept[Exception] {
        spark.sql("UPDATE T SET status = 'ready' WHERE id = 48").collect()
      }
      assert(nearest("status IS NULL") == Seq(48))
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('global-index.column-update-action'='IGNORE')")
      intercept[Exception] {
        spark.sql("UPDATE T SET status = 'ready' WHERE id = 48").collect()
      }
      spark.sql(
        "ALTER TABLE T SET TBLPROPERTIES ('global-index.column-update-action'='DROP_PARTITION_INDEX')")
      spark.sql("UPDATE T SET status = 'ready' WHERE id = 48").collect()
      assert(files.isEmpty)

      build("""{"status":"bitmap"}""")
      assert(nearest("status = 'ready'") == Seq(48))
      val centroidFiles = files.map(_.fileName()).toSet
      spark.sql("INSERT INTO T VALUES (64, array(2000.0f, 0.0f), 'red', 'new')").collect()
      build(sharding = "range")
      assert(centroidFiles.subsetOf(files.map(_.fileName()).toSet))
      assert(files.size == centroidFiles.size + 1)
      assert(nearest("status = 'ready'") == Seq(48))
      spark
        .sql("""CALL sys.drop_global_index(table=>'test.T',index_column=>'v',
               |index_type=>'ivf-flat')""".stripMargin)
        .collect()
      assert(files.isEmpty)
    }
  }

  test("distributed centroid search applies the shard bitmap before top K") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>, status STRING, p INT)
                  |PARTITIONED BY (p) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                  |'global-index.thread-num'='1',
                  |'vector-index.post-filter.candidates-factor'='1')""".stripMargin)
      spark.sql(
        """INSERT INTO T VALUES
          |(0, array(0.0f, 0.0f), 'near', 0), (1, array(1.0f, 0.0f), NULL, 0),
          |(2, array(0.0f, 0.0f), 'near', 1), (3, array(2.0f, 0.0f), NULL, 1)""".stripMargin)
      spark
        .sql("""CALL sys.create_global_index(
               |table => 'test.T', index_column => 'v', index_type => 'ivf-flat',
               |options => 'ivf-flat.sharding=centroid,ivf-flat.dimension=2,ivf-flat.metric=l2,ivf-flat.centroid.nlist=1',
               |extra_fields => '{"status":"bitmap"}')""".stripMargin)
        .collect()
      withSparkSQLConf("spark.paimon.vector-search.distribute.enabled" -> "true") {
        val ids = spark
          .sql("""SELECT id FROM vector_search('T','v',array(0.0f,0.0f),1,
                 |map('centroid.nprobe','1','ivf.nprobe','1')) WHERE status IS NULL""".stripMargin)
          .collect()
          .map(_.getInt(0))
        assert(ids.toSeq == Seq(1))
      }
    }
  }

  private def build(): Unit = {
    spark
      .sql(
        """CALL sys.create_global_index(
          |table => 'test.T', index_column => 'v', index_type => 'ivf-pq',
          |partitions => 'p=0',
          |options => 'ivf-pq.sharding=centroid,ivf-pq.centroid.nlist=4,ivf-pq.pq.m=1')""".stripMargin)
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

  test("automatic inner product routing preserves maximum scores under query scaling") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true',
                  |'data-evolution.enabled'='true')""".stripMargin)
      spark.sql("""INSERT INTO T SELECT cast(id AS INT), array(cast(
                  |(CASE WHEN id<32 THEN 1 WHEN id<64 THEN 2 WHEN id<96 THEN 4 ELSE 100 END)
                  |+ 0.001 * (id % 32) AS FLOAT), 0.0f) FROM range(128)""".stripMargin)
      spark.sql(
        """CALL sys.create_global_index(table=>'test.T',index_column=>'v',
          |index_type=>'ivf-flat', options=>'ivf-flat.sharding=centroid,ivf-flat.dimension=2,
          |ivf-flat.centroid.nlist=4,ivf-flat.nlist=1,
          |ivf-flat.train.sample-ratio=1')""".stripMargin.replace("\n", ""))
      Seq("1.0f", "100.0f").foreach {
        q =>
          val ids = spark
            .sql(s"""SELECT id FROM vector_search('T','v',array($q,0.0f),10)
                    |ORDER BY id""".stripMargin)
            .collect()
            .map(_.getInt(0))
            .toSeq
          assert(ids == (118 until 128))
      }
    }
  }

  test("cosine refinement uses the build metric for scalar and batch search") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, v ARRAY<FLOAT>, p INT) PARTITIONED BY (p) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true',
                  |'data-evolution.enabled'='true')""".stripMargin)
      spark.sql("INSERT INTO T VALUES (0,cast(NULL AS ARRAY<FLOAT>),0)")
      spark.sql(
        """CALL sys.create_global_index(table=>'test.T',index_column=>'v',
          |index_type=>'ivf-pq',partitions=>'p=0',
          |options=>'ivf-pq.sharding=centroid,ivf-pq.dimension=2,ivf-pq.pq.m=1')""".stripMargin)
      spark.sql("""INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES
                  |(1,array(1.0f,0.0f),1),(2,array(10.0f,10.0f),1) AS x(id,v,p)""".stripMargin)
      // The empty marker still covers p=0. It must not supply its inner-product metric
      // to an exact search over p=1, with either the default L2 or an explicit cosine metric.
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('vector-index.search-mode'='detail')")
      def rawNearest(): Int = spark
        .sql("SELECT id FROM vector_search('T','v',array(1.0f,0.0f),1)")
        .collect()
        .head
        .getInt(0)
      assert(rawNearest() == 1)
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('fields.v.metric'='cosine')")
      assert(rawNearest() == 1)
      val rawBatch = loadTable("T")
        .newBatchVectorSearchBuilder()
        .withVectorColumn("v")
        .withVectors(Array(Array(1.0f, 0.0f), Array(0.0f, 1.0f)))
        .withLimit(1)
        .executeBatchLocal()
      assert(rawBatch.get(0).results().toArray(1).toSeq == Seq(1L))
      assert(rawBatch.get(1).results().toArray(1).toSeq == Seq(2L))
      spark.sql("ALTER TABLE T UNSET TBLPROPERTIES ('fields.v.metric')")
      spark.sql(
        """CALL sys.create_global_index(table=>'test.T',index_column=>'v',
          |index_type=>'ivf-pq',partitions=>'p=1',options=>'ivf-pq.sharding=centroid,ivf-pq.dimension=2,
          |ivf-pq.distance.metric=cosine,ivf-pq.centroid.nlist=1,
          |ivf-pq.nlist=1,ivf-pq.pq.m=1')""".stripMargin
          .replace("\n", ""))
      val query = Array(1.0f, 0.0f)
      val ids = spark
        .sql("""SELECT id FROM vector_search('T','v',array(1.0f,0.0f),1,
               |map('ivf.refine_factor','2'))""".stripMargin)
        .collect()
      assert(ids.map(_.getInt(0)).toSeq == Seq(1))
      val batch = loadTable("T")
        .newBatchVectorSearchBuilder()
        .withVectorColumn("v")
        .withVectors(Array(query, Array(0.0f, 1.0f)))
        .withLimit(1)
        .withOption("ivf.refine_factor", "2")
        .executeBatchLocal()
      assert(batch.get(0).results().toArray(1).toSeq == Seq(1L))
      assert(batch.get(1).results().toArray(1).toSeq == Seq(2L))
    }
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
                  |'global-index.thread-num' = '1',
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
      val byRange = indexes.groupBy(
        f => (f.globalIndexMeta().rowRangeStart(), f.globalIndexMeta().rowRangeEnd()))
      assert(byRange.keySet == Set((64L, 191L), (320L, 447L)))
      byRange.values.foreach {
        group =>
          assert(group.size == 5)
          assert(group.count(CentroidIndexMeta.isRoutingModel) == 1)
      }
      assert(nearest(300) == 300)
      // Partition 1 lies inside the indexed partition's min/max envelope, but is unindexed.
      assert(nearest(200) == 200)

      build()
      assert(indexes.size == 10)
      assert(indexes.map(_.fileName()).toSet.intersect(firstFiles).isEmpty)
      assert(nearest(300) == 300)
      assert(nearest(200) == 200)

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
      val btreeRanges = table
        .store()
        .newIndexFileHandler()
        .scan(table.snapshotManager().latestSnapshot(), "btree")
        .asScala
        .filter(_.partition().getInt(0) == 0)
        .map(_.indexFile().globalIndexMeta().rowRange())
        .toSet
      assert(btreeRanges == indexes.map(_.globalIndexMeta().rowRange()).toSet)
      assert(filteredIds() == Seq(300))
      spark.sql("DELETE FROM T WHERE p = 0 AND id = 300")
      assert(nearest(300) != 300)

      spark.sql("INSERT INTO T VALUES (999, CAST(NULL AS ARRAY<FLOAT>), 3)")
      spark
        .sql("""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
               |index_type => 'ivf-pq',
               |options => 'ivf-pq.sharding=centroid,ivf-pq.centroid.nlist=4,ivf-pq.pq.m=1')""".stripMargin)
        .collect()
      val routers = indexes.filter(CentroidIndexMeta.isRoutingModel)
      assert(
        routers
          .map(f => (f.globalIndexMeta().rowRangeStart(), f.globalIndexMeta().rowRangeEnd()))
          .toSet ==
          Set((0L, 63L), (64L, 191L), (192L, 319L), (320L, 447L), (448L, 448L)))
      assert(routers.size == 5)
      assert(indexes.count(_.globalIndexMeta().rowRangeStart() == 448L) == 1)
      assert(nearest(200) == 200)

      def fileRanges = indexes.map(f => f.fileName() -> f.globalIndexMeta().rowRange()).toMap
      val before = fileRanges
      assert(new DataEvolutionRowIdReassigner(table).reassign().reassigned)
      val after = fileRanges
      assert(after.keySet == before.keySet)
      assert(after != before)
      assert(nearest(301) == 301)
      assert(nearest(300) != 300)
    }
  }
  Seq(true, false).foreach {
    centroidFirst =>
      test(
        s"range and centroid partitions share vector search with centroid built first=$centroidFirst") {
        withTable("T") {
          spark.sql(
            """CREATE TABLE T (id INT, v ARRAY<FLOAT>, p INT) PARTITIONED BY (p)
              |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
              |'data-evolution.enabled'='true', 'vector-index.search-mode'='detail',
              |'fields.v.dimension'='2', 'fields.v.metric'='l2',
              |'global-index.thread-num'='1', 'global-index.row-count-per-shard'='32')""".stripMargin)
          Seq(0, 1, 2).foreach {
            part =>
              val first = if (part == 2) 5000 else 0
              spark.sql(s"""INSERT INTO T SELECT CAST(id + ${part * 128} AS INT),
                           |array(CAST(IF(id = 0, $first, 1000 + id) AS FLOAT), 0.0f), $part
                           |FROM range(0, 128, 1, 1)""".stripMargin)
          }
          // Give the routed layout two disjoint ranges so the SQL query opens readers on workers.
          spark.sql("INSERT INTO T VALUES (9000, array(9000.0f,0.0f), 0)")
          val builds = Seq(("centroid", 0), ("range", 1))
          (if (centroidFirst) builds else builds.reverse).foreach {
            case (layout, part) =>
              spark
                .sql(
                  s"""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
                     |index_type => 'ivf-pq', partitions => 'p=$part',
                     |options => 'ivf-pq.sharding=$layout,ivf-pq.centroid.nlist=2,ivf-pq.nlist=2,ivf-pq.pq.m=1')""".stripMargin)
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

  test("a large centroid range uses the map slots when built with a singleton range") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (v ARRAY<FLOAT>, p INT) PARTITIONED BY (p) TBLPROPERTIES (
                  |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                  |'global-index.row-count-per-shard'='16',
                  |'global-index.build.max-parallelism'='2')""".stripMargin)
      spark.sql("""INSERT INTO T PARTITION(p=0) SELECT array(cast(id AS FLOAT),1.0f)
                  |FROM range(256)""".stripMargin)
      spark.sql("INSERT INTO T PARTITION(p=1) VALUES (array(1.0f,1.0f))")

      val sc = spark.sparkContext
      val group = s"centroid-skew-${System.nanoTime()}"
      sc.setJobGroup(group, group)
      try {
        spark
          .sql("""CALL sys.create_global_index(table=>'test.T', index_column=>'v',
                 |index_type=>'ivf-flat', options=>'ivf-flat.sharding=centroid,ivf-flat.dimension=2,
                 |ivf-flat.centroid.nlist=2')""".stripMargin.replace("\n", ""))
          .collect()
      } finally {
        sc.clearJobGroup()
      }
      val tracker = sc.statusTracker
      val mapWidths = tracker
        .getJobIdsForGroup(group)
        .flatMap(id => tracker.getJobInfo(id).toSeq.flatMap(_.stageIds))
        .distinct
        .flatMap(id => tracker.getStageInfo(id).toSeq)
        .filter(_.name.startsWith("mapPartitionsWithIndex at CentroidGlobalIndexTopoBuilder"))
        .map(_.numTasks)
      assert(mapWidths.sorted.toSeq == Seq(1, 2))
    }
  }

  test("distributed training preserves the model across sparse splits and different task counts") {
    withTable("T") {
      spark.sql(
        """CREATE TABLE T (id INT, v ARRAY<FLOAT>) TBLPROPERTIES (
          |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
          |'deletion-vectors.enabled'='true', 'vector-index.search-mode'='fast',
          |'fields.v.index-options'='{"sharding":"centroid"}', 'ivf-flat.dimension'='2', 'ivf-flat.metric'='l2',
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
            |options => 'ivf-flat.centroid.nlist=2,ivf-flat.train.sample-ratio=0.125')""".stripMargin)
        val table = loadTable("T")
        val model = table
          .store()
          .newIndexFileHandler()
          .scan(table.snapshotManager().latestSnapshot(), "ivf-flat")
          .asScala
          .map(_.indexFile())
          .find(f => CentroidIndexMeta.isRoutingModel(f))
          .get
        val meta = CentroidIndexMeta
          .file(model, table.store().pathFactory().globalIndexFileFactory())
          .asInstanceOf[CentroidIndexFile]
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
          def create(layout: String): Unit = {
            val pq = if (kind == "ivf-pq" || kind == "diskann") s",$kind.pq.m=1" else ""
            spark
              .sql(
                s"""CALL sys.create_global_index(table => 'test.T', index_column => 'v',
                   |index_type => '$kind', options => '$kind.sharding=$layout,$kind.centroid.nlist=4$pq')""".stripMargin)
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
          create("centroid")
          val table = loadTable("T")
          val files = table
            .store()
            .newIndexFileHandler()
            .scan(table.snapshotManager().latestSnapshot(), kind)
            .asScala
            .map(_.indexFile())
          assert(files.size == 5 && files.count(f => CentroidIndexMeta.isRoutingModel(f)) == 1)
          assert(nearest("centroid-based").toSeq == Seq(200))
          val centroids = files
            .filterNot(f => CentroidIndexMeta.isRoutingModel(f))
            .map(
              f =>
                CentroidIndexMeta
                  .file(f, table.store().pathFactory().globalIndexFileFactory())
                  .asInstanceOf[CentroidIndexFile]
                  .centroid())
          assert(centroids.toSet == Set(0, 1, 2, 3))
          spark
            .sql(s"""CALL sys.drop_global_index(table => 'test.T', index_column => 'v',
                    |index_type => '$kind')""".stripMargin)
            .collect()
          spark.sql(s"ALTER TABLE T SET TBLPROPERTIES ('$kind.sharding'='centroid')")
          create("range")
          val rangeFiles = table
            .store()
            .newIndexFileHandler()
            .scan(table.snapshotManager().latestSnapshot(), kind)
            .asScala
          assert(rangeFiles.nonEmpty)
          assert(
            rangeFiles.forall(
              f => !CentroidIndexMeta.isWrapped(f.indexFile().globalIndexMeta().indexMeta())))
          assert(nearest("range-based").toSeq == Seq(200))
        }
      }
  }

}

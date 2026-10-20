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

import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase}
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackDataSplit
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConverters._

abstract class PhysicalBucketPushDownTestBase extends PaimonSparkTestBase {

  Seq(false, true).foreach {
    hasPrimaryKey =>
      test(s"physical bucket predicates prune data files: hasPrimaryKey=$hasPrimaryKey") {
        withTable("T") {
          createFixedBucketTable(hasPrimaryKey)
          if (hasPrimaryKey) {
            sql("INSERT INTO T SELECT CAST(id AS INT), 'updated' FROM range(0, 100)")
          }
          val all = sql("SELECT id, payload, __paimon_bucket FROM T")
          val reference = materialize(all)
          val allSplits = scanOf(all).inputSplits.map(_.asInstanceOf[DataSplit])
          assert(allSplits.map(_.bucket()).toSet == Set(0, 1, 2, 3))

          // Exercise SQL conversion once; the unit tests cover the NULL/boolean matrix.
          val extraConditions = if (hasPrimaryKey) {
            Seq.empty
          } else {
            // Type NULL explicitly to preserve metadata columns in older Spark analyzers.
            Seq(
              "__paimon_bucket IN (1, CAST(NULL AS INT), 3)" -> Set(1, 3),
              "__paimon_bucket <=> 1" -> Set(1),
              "__paimon_bucket NOT IN (1, 3)" -> Set(0, 2),
              "__paimon_bucket = 1 OR __paimon_bucket = 3" -> Set(1, 3),
              "__paimon_bucket = 1 OR id = 42" -> Set(0, 1, 2, 3)
            )
          }
          val conditions = Seq(
            "__paimon_bucket = 1" -> Set(1),
            "__paimon_bucket IN (1, 3)" -> Set(1, 3),
            "__paimon_bucket >= 1 AND __paimon_bucket < 3" -> Set(1, 2),
            "__paimon_bucket = 99" -> Set.empty[Int]
          ) ++ extraConditions
          conditions.foreach {
            case (condition, expectedBuckets) =>
              val query = s"SELECT id, payload, __paimon_bucket FROM T WHERE $condition"
              val scan = checkAnswerAndGetScan(query, reference.filter(expr(condition)))
              assertBucketFiles(scan, expectedBuckets, allSplits)
          }

          if (!hasPrimaryKey) {
            // Isolate the unsupported expression from inferred IS NOT NULL predicates, which can
            // independently be pushed down as bucket filters.
            withSparkSQLConf("spark.sql.constraintPropagation.enabled" -> "false") {
              val condition = "__paimon_bucket % 2 = 0"
              val query = s"SELECT id, payload, __paimon_bucket FROM T WHERE $condition"
              val scan = checkAnswerAndGetScan(query, reference.filter(expr(condition)))
              assert(scan.pushedBucketFilters.isEmpty)
              assertBucketFiles(scan, Set(0, 1, 2, 3), allSplits)
            }
          }

          if (hasPrimaryKey) {
            val condition = "__paimon_bucket = 1 AND payload = 'updated'"
            val query = s"SELECT id, payload, __paimon_bucket FROM T WHERE $condition"
            val expected = reference.filter(expr(condition)).collect().toSeq
            assert(expected.nonEmpty)
            val actual = sql(query)
            checkAnswer(actual, expected)
            val scan = scanOf(actual)
            assert(scan.pushedBucketFilters.nonEmpty)
            assert(scan.pushedDataFilters.nonEmpty)
            assert(scan.inputSplits.map(_.asInstanceOf[DataSplit].bucket()).toSet == Set(1))
          }

          // Filtering must also work when the metadata column is not selected, and must not be
          // bypassed by metadata-only aggregation or TopN/limit pushdown.
          val selected = reference.filter("__paimon_bucket = 1")
          Seq(
            "SELECT id FROM T WHERE __paimon_bucket = 1" -> selected.select("id"),
            "SELECT count(*) FROM T WHERE __paimon_bucket = 1" -> selected.selectExpr("count(*)"),
            "SELECT id FROM T WHERE __paimon_bucket = 1 ORDER BY id LIMIT 5" ->
              selected.select("id").orderBy("id").limit(5)
          ).foreach {
            case (query, expected) =>
              val scan = checkAnswerAndGetScan(query, expected)
              assert(scan.pushedBucketFilters.nonEmpty)
              assert(scan.pushedTopN.isEmpty)
              assertBucketFiles(scan, Set(1), allSplits)
          }

          if (!hasPrimaryKey) {
            val bucketOneScan = getPaimonScan("SELECT id FROM T WHERE __paimon_bucket = 1")
            val bucketThreeScan = getPaimonScan("SELECT id FROM T WHERE __paimon_bucket = 3")
            val copiedScan = bucketOneScan.copy()
            assert(bucketOneScan != bucketThreeScan)
            assert(copiedScan == bucketOneScan)
            assertBucketFiles(copiedScan, Set(1), allSplits)
            assert(bucketOneScan.description().contains("BucketFilters:"))
          }
        }
      }

      test(
        s"physical bucket predicates intersect bucket-key equality: hasPrimaryKey=$hasPrimaryKey") {
        withTable("T") {
          createFixedBucketTable(hasPrimaryKey)
          val reference = materialize(sql("SELECT id, __paimon_bucket FROM T"))
          val expectedKey = reference.filter("id = 42")
          val keyBucket = expectedKey.select("__paimon_bucket").head().getInt(0)
          val otherBuckets = (0 until 4).filter(_ != keyBucket)
          val keyQuery = "SELECT id, __paimon_bucket FROM T WHERE id = 42"
          val keySplits = getPaimonScan(keyQuery).inputSplits.map(_.asInstanceOf[DataSplit])
          assert(keySplits.map(_.bucket()).toSet == Set(keyBucket))

          Seq(
            s"__paimon_bucket = $keyBucket" -> Set(keyBucket),
            s"__paimon_bucket = ${otherBuckets.head}" -> Set.empty[Int],
            s"__paimon_bucket IN ($keyBucket, ${otherBuckets.head})" -> Set(keyBucket),
            s"__paimon_bucket IN (${otherBuckets(0)}, ${otherBuckets(1)})" -> Set.empty[Int]
          ).foreach {
            case (condition, expectedBuckets) =>
              val query = s"$keyQuery AND $condition"
              val scan = checkAnswerAndGetScan(query, expectedKey.filter(expr(condition)))
              assert(scan.pushedBucketFilters.nonEmpty, condition)
              assert(scan.pushedDataFilters.nonEmpty, condition)
              assertBucketFiles(scan, expectedBuckets, keySplits)
          }
        }
      }

      test(s"physical bucket predicates with unordered limit: hasPrimaryKey=$hasPrimaryKey") {
        withTable("T") {
          createFixedBucketTable(hasPrimaryKey)
          val reference = materialize(sql("SELECT id, __paimon_bucket FROM T"))
          assert(reference.filter("__paimon_bucket = 1").count() > 5)

          Seq(1, 99).foreach {
            bucket =>
              val query = s"SELECT id FROM T WHERE __paimon_bucket = $bucket LIMIT 5"
              val expected = reference
                .filter(s"__paimon_bucket = $bucket")
                .select("id")
                .collect()
                .toSet
              val result = sql(query)
              val actual = result.collect().toSeq
              // LIMIT without ORDER BY may return any subset of the matching rows.
              assert(actual.size == math.min(5, expected.size), query)
              assert(actual.distinct.size == actual.size, query)
              assert(actual.forall(expected.contains), query)

              val scan = scanOf(result)
              assert(scan.pushedBucketFilters.nonEmpty, query)
              val splits = scan.inputSplits.map(_.asInstanceOf[DataSplit])
              val expectedBuckets = if (expected.isEmpty) Set.empty[Int] else Set(bucket)
              assert(splits.map(_.bucket()).toSet == expectedBuckets, query)
          }
        }
      }
  }

  test("physical bucket predicates prune files with unsupported residual expressions") {
    withTable("T") {
      createFixedBucketTable(hasPrimaryKey = false)
      val all = sql("SELECT id, payload, __paimon_bucket FROM T")
      val reference = materialize(all)
      val allSplits = scanOf(all).inputSplits.map(_.asInstanceOf[DataSplit])
      assert(allSplits.map(_.bucket()).toSet == Set(0, 1, 2, 3))

      // Disable inferred IS NOT NULL filters so only the bucket predicate can be pushed down.
      withSparkSQLConf("spark.sql.constraintPropagation.enabled" -> "false") {
        val bucketCondition = "__paimon_bucket IN (1, 3)"
        val bucketRows = reference.filter(bucketCondition).collect().toSeq
        val expected = bucketRows.filter(_.getInt(0) % 2 == 0)
        assert(expected.nonEmpty && expected.size < bucketRows.size)

        val actual =
          sql(s"SELECT id, payload, __paimon_bucket FROM T WHERE $bucketCondition AND id % 2 = 0")
        checkAnswer(actual, expected)
        val scan = scanOf(actual)
        assert(scan.pushedBucketFilters.nonEmpty)
        assert(scan.pushedDataFilters.isEmpty)
        assertBucketFiles(scan, Set(1, 3), allSplits)
      }
    }
  }

  if (gteqSpark3_3) {
    test("physical bucket predicates preserve grouped aggregates and joins") {
      withTable("T") {
        createFixedBucketTable(hasPrimaryKey = true)
        sql("INSERT INTO T SELECT CAST(id AS INT), 'updated' FROM range(100)")
        val all = sql("SELECT id, payload, __paimon_bucket FROM T")
        val reference = materialize(all)
        val allSplits = scanOf(all).inputSplits.map(_.asInstanceOf[DataSplit])
        assert(allSplits.map(_.bucket()).toSet == Set(0, 1, 2, 3))

        withSparkSQLConf(
          "spark.paimon.scan.preserve-data-grouping" -> "true",
          "spark.sql.sources.v2.bucketing.enabled" -> "true",
          "spark.sql.adaptive.enabled" -> "false",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1"
        ) {
          Seq(
            "__paimon_bucket IN (1, 3)" -> Set(1, 3),
            "__paimon_bucket = 99" -> Set.empty[Int]
          ).foreach {
            case (condition, expectedBuckets) =>
              val query = s"SELECT id, count(*) FROM T WHERE $condition GROUP BY id"
              val scan =
                checkAnswerAndGetScan(
                  query,
                  reference.filter(expr(condition)).groupBy("id").count())
              assert(scan.pushedBucketFilters.nonEmpty, condition)
              assertBucketFiles(scan, expectedBuckets, allSplits)
              assert(scan.inputPartitions.nonEmpty == expectedBuckets.nonEmpty, condition)
              assert(scan.inputPartitions.forall(_.bucketed), condition)
          }

          // The two scans retain different bucket sets, so the join must align matching keys.
          // Spark 3.4+ can fill missing bucket partitions without shuffling either side.
          val pushPartValuesOptions = if (gteqSpark3_4) Seq(false, true) else Seq(false)
          pushPartValuesOptions.foreach {
            pushPartValues =>
              withSparkSQLConf(
                "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> pushPartValues.toString) {
                val joined =
                  sql("""SELECT l.id, l.payload, r.payload FROM T l JOIN T r ON l.id = r.id
                        |WHERE l.__paimon_bucket IN (1, 3) AND r.__paimon_bucket IN (0, 1)
                        |""".stripMargin)
                checkAnswer(
                  joined,
                  reference.filter("__paimon_bucket = 1").selectExpr("id", "payload", "payload"))
                val plan = joined.queryExecution.executedPlan
                val shuffles = plan.collect { case exchange: ShuffleExchangeLike => exchange }
                assert(
                  shuffles.isEmpty == pushPartValues,
                  s"pushPartValues=$pushPartValues:\n$plan")

                val scans = joined.queryExecution.optimizedPlan.collect {
                  case relation: DataSourceV2ScanRelation => relation.scan.asInstanceOf[PaimonScan]
                }
                assert(scans.size == 2)
                val scanBuckets = scans.map {
                  scan => (scan, scan.inputSplits.map(_.asInstanceOf[DataSplit].bucket()).toSet)
                }
                assert(scanBuckets.map(_._2).toSet == Set(Set(1, 3), Set(0, 1)))
                scanBuckets.foreach {
                  case (scan, buckets) =>
                    assert(scan.pushedBucketFilters.nonEmpty)
                    assertBucketFiles(scan, buckets, allSplits)
                    assert(scan.inputPartitions.nonEmpty)
                    assert(scan.inputPartitions.forall(_.bucketed))
                }
              }
          }
        }
      }
    }
  }

  test("physical bucket filters preserve partition filters and historical bucket IDs") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, p INT) PARTITIONED BY (p)
            |TBLPROPERTIES ('bucket' = '4', 'bucket-key' = 'id')""".stripMargin)
      sql("INSERT INTO T SELECT CAST(id AS INT), 0 FROM range(400)")
      sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '2')")
      sql("INSERT INTO T SELECT CAST(id AS INT), 1 FROM range(400)")
      val all = sql("SELECT id, p, __paimon_bucket FROM T")
      val reference = materialize(all)
      val allSplits = scanOf(all).inputSplits.map(_.asInstanceOf[DataSplit])
      Seq(
        "__paimon_bucket = 3" -> Set((0, 3)),
        "__paimon_bucket IN (1, 3)" -> Set((0, 1), (0, 3), (1, 1)),
        "__paimon_bucket >= 2" -> Set((0, 2), (0, 3))
      ).foreach {
        case (condition, expectedPartitionBuckets) =>
          val query = s"SELECT id, p, __paimon_bucket FROM T WHERE $condition"
          val scan = checkAnswerAndGetScan(query, reference.filter(expr(condition)))
          val splits = scan.inputSplits.map(_.asInstanceOf[DataSplit])
          assert(
            splits
              .map(s => (s.partition().getInt(0), s.bucket()))
              .toSet == expectedPartitionBuckets,
            condition)
          val expectedSplits = allSplits.filter(
            s => expectedPartitionBuckets.contains((s.partition().getInt(0), s.bucket())))
          assert(dataFileNames(splits) == dataFileNames(expectedSplits), condition)
      }

      val query =
        "SELECT id, p, __paimon_bucket FROM T WHERE p = 1 AND __paimon_bucket = 1 AND id >= 100"
      val scan =
        checkAnswerAndGetScan(
          query,
          reference.filter(expr("p = 1 AND __paimon_bucket = 1 AND id >= 100")))
      val splits = scan.inputSplits.map(_.asInstanceOf[DataSplit])
      assert(splits.nonEmpty)
      assert(splits.forall(s => s.bucket() == 1 && s.partition().getInt(0) == 1))
    }
  }

  test("physical bucket predicates preserve fallback partition ownership") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, p INT, payload STRING) PARTITIONED BY (p)
            |TBLPROPERTIES ('bucket' = '4', 'bucket-key' = 'id')""".stripMargin)
      sql("CALL sys.create_branch('test.T', 'fallback')")
      sql("""INSERT INTO `T$branch_fallback`
            |SELECT CAST(ids.id AS INT), CAST(parts.id AS INT), 'fallback'
            |FROM range(100) ids CROSS JOIN range(3) parts""".stripMargin)
      val fallback =
        materialize(sql("SELECT id, p, payload, __paimon_bucket FROM `T$branch_fallback`"))

      sql("INSERT INTO T SELECT CAST(id AS INT), 0, 'main' FROM range(100)")
      // The main branch owns partition 2, but has no rows in bucket 1. Bucket pruning must not
      // make it fall back to the older partition, which does contain rows in that bucket.
      val bucketZeroId =
        fallback.filter("p = 2 AND __paimon_bucket = 0").select("id").head().getInt(0)
      sql(s"INSERT INTO T VALUES ($bucketZeroId, 2, 'main')")
      val main = materialize(sql("SELECT id, p, payload, __paimon_bucket FROM T"))
      val reference = main.union(fallback.filter("p = 1"))
      assert(fallback.filter("p = 2 AND __paimon_bucket = 1").count() > 0)

      sql("ALTER TABLE T SET TBLPROPERTIES ('scan.fallback-branch' = 'fallback')")
      val query = "SELECT id, p, payload, __paimon_bucket FROM T"
      val allSplits =
        checkAnswerAndGetScan(query, reference).inputSplits.map(_.asInstanceOf[FallbackDataSplit])
      assert(
        allSplits.filter(!_.isFallback).map(s => (s.partition().getInt(0), s.bucket())).toSet ==
          (0 until 4).map(b => (0, b)).toSet + ((2, 0)))
      assert(
        allSplits.filter(_.isFallback).map(s => (s.partition().getInt(0), s.bucket())).toSet ==
          (0 until 4).map(b => (1, b)).toSet)

      Seq(
        "__paimon_bucket = 1" -> Set(1),
        "__paimon_bucket IN (1, 3)" -> Set(1, 3)
      ).foreach {
        case (condition, expectedBuckets) =>
          val scan =
            checkAnswerAndGetScan(s"$query WHERE $condition", reference.filter(expr(condition)))
          assert(scan.pushedBucketFilters.nonEmpty, condition)
          val splits = scan.inputSplits.map(_.asInstanceOf[FallbackDataSplit])
          val expectedSplits = allSplits.filter(s => expectedBuckets.contains(s.bucket()))
          assert(
            splits.map(s => (s.partition().getInt(0), s.bucket(), s.isFallback)).toSet ==
              expectedSplits.map(s => (s.partition().getInt(0), s.bucket(), s.isFallback)).toSet,
            condition
          )
          assert(dataFileNames(splits) == dataFileNames(expectedSplits), condition)
      }

      val missingBucket = sql(s"$query WHERE p = 2 AND __paimon_bucket = 1")
      checkAnswer(missingBucket, Seq.empty[Row])
      assert(scanOf(missingBucket).inputSplits.isEmpty)
    }
  }

  test("physical bucket predicates preserve the configured scan bucket") {
    withTable("T") {
      createFixedBucketTable(hasPrimaryKey = true)
      sql("ALTER TABLE T SET TBLPROPERTIES ('scan.bucket' = '0')")
      val reference = materialize(sql("SELECT id, __paimon_bucket FROM T"))
      assert(reference.collect().map(_.getInt(1)).toSet == Set(0))

      Seq(
        "__paimon_bucket = 0" -> Set(0),
        "__paimon_bucket = 1" -> Set.empty[Int],
        "__paimon_bucket IN (0, 1)" -> Set(0),
        "__paimon_bucket IN (1, 3)" -> Set.empty[Int]
      ).foreach {
        case (condition, expectedBuckets) =>
          val query = s"SELECT id, __paimon_bucket FROM T WHERE $condition"
          val scan = checkAnswerAndGetScan(query, reference.filter(expr(condition)))
          val splits = scan.inputSplits.map(_.asInstanceOf[DataSplit])
          assert(splits.map(_.bucket()).toSet == expectedBuckets, condition)
      }
    }
  }

  Seq(false, true).foreach {
    hasPrimaryKey =>
      test(s"physical bucket predicates with bucket=-1: hasPrimaryKey=$hasPrimaryKey") {
        withTable("T") {
          val primaryKeyOptions = if (hasPrimaryKey) {
            """, 'primary-key' = 'id', 'dynamic-bucket.initial-buckets' = '3',
              |'dynamic-bucket.assigner-parallelism' = '3'""".stripMargin
          } else {
            ""
          }
          sql(s"""CREATE TABLE T (id INT, payload INT)
                 |TBLPROPERTIES ('bucket' = '-1' $primaryKeyOptions)""".stripMargin)
          sql("INSERT INTO T SELECT CAST(id AS INT), CAST(id AS INT) FROM range(100)")
          if (hasPrimaryKey) {
            sql("INSERT INTO T SELECT CAST(id AS INT), -1 FROM range(10)")
          }

          val all = sql("SELECT id, payload, __paimon_bucket FROM T")
          val reference = materialize(all)
          val allSplits = scanOf(all).inputSplits.map(_.asInstanceOf[DataSplit])
          val physicalBuckets = allSplits.map(_.bucket()).toSet
          // bucket=-1 selects the bucket mode; stored files still have non-negative physical IDs.
          if (hasPrimaryKey) {
            assert(physicalBuckets.size > 1 && physicalBuckets.forall(_ >= 0))
          } else {
            assert(physicalBuckets == Set(0))
          }
          assert(reference.collect().map(_.getInt(2)).toSet == physicalBuckets)
          val selectedBucket = physicalBuckets.min
          Seq(
            s"__paimon_bucket = $selectedBucket" -> Set(selectedBucket),
            s"__paimon_bucket > $selectedBucket" -> (physicalBuckets - selectedBucket),
            "__paimon_bucket = -1" -> Set.empty[Int]
          ).foreach {
            case (condition, expectedBuckets) =>
              val query = s"SELECT id, payload, __paimon_bucket FROM T WHERE $condition"
              val scan = checkAnswerAndGetScan(query, reference.filter(expr(condition)))
              assert(scan.pushedBucketFilters.nonEmpty, condition)
              assertBucketFiles(scan, expectedBuckets, allSplits)
          }
        }
      }
  }

  test("physical bucket predicates prune postpone tables in compacted-full mode") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, payload STRING) TBLPROPERTIES (
            |'primary-key' = 'id', 'bucket-key' = 'id', 'bucket' = '-2',
            |'postpone.default-bucket-num' = '4',
            |'postpone.batch-write-fixed-bucket' = 'false')""".stripMargin)
      sql("INSERT INTO T SELECT CAST(id AS INT), 'compacted' FROM range(400)")
      checkAnswer(sql("SELECT DISTINCT bucket FROM `T$buckets`"), Seq(Row(-2)))
      sql("CALL sys.compact(table => 'T')")

      val reference = materialize(sql("SELECT id, payload, __paimon_bucket FROM T"))
      checkAnswer(reference.select("id", "payload"), (0 until 400).map(id => Row(id, "compacted")))

      // Leave both updates and new keys in the postpone bucket after the compacted snapshot.
      sql("INSERT INTO T SELECT CAST(id AS INT), 'pending' FROM range(410)")
      checkAnswer(
        sql("SELECT DISTINCT bucket FROM `T$buckets`"),
        Seq(Row(-2), Row(0), Row(1), Row(2), Row(3)))
      checkAnswer(sql("SELECT record_count FROM `T$buckets` WHERE bucket = -2"), Seq(Row(410L)))

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.paimon.scan.mode" -> "compacted-full") {
        val allScan = checkAnswerAndGetScan("SELECT id, payload, __paimon_bucket FROM T", reference)
        assert(allScan.planPostponeMerge().isEmpty)
        val allSplits = allScan.inputSplits.map(_.asInstanceOf[DataSplit])
        assert(allSplits.map(_.bucket()).toSet == Set(0, 1, 2, 3))

        Seq(
          "__paimon_bucket = 1" -> Set(1),
          "__paimon_bucket IN (1, 3)" -> Set(1, 3),
          "__paimon_bucket = -2" -> Set.empty[Int]
        ).foreach {
          case (condition, expectedBuckets) =>
            val query = s"SELECT id, payload, __paimon_bucket FROM T WHERE $condition"
            val scan = checkAnswerAndGetScan(query, reference.filter(expr(condition)))
            assert(scan.planPostponeMerge().isEmpty, condition)
            assert(scan.pushedBucketFilters.nonEmpty, condition)
            assertBucketFiles(scan, expectedBuckets, allSplits)
        }
      }
    }
  }

  test("a business column named __paimon_bucket is not a physical bucket predicate") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, __paimon_bucket INT)
            |TBLPROPERTIES ('bucket' = '4', 'bucket-key' = 'id')""".stripMargin)
      sql("INSERT INTO T SELECT CAST(id AS INT), 99 FROM range(100)")
      val actual = sql("SELECT * FROM T WHERE __paimon_bucket = 99")
      assert(actual.collect().length == 100)
      val scan = scanOf(actual)
      assert(scan.pushedBucketFilters.isEmpty)
      assert(scan.inputSplits.map(_.asInstanceOf[DataSplit].bucket()).toSet == Set(0, 1, 2, 3))
    }
  }

  private def checkAnswerAndGetScan(query: String, expected: DataFrame): PaimonScan = {
    val actual = sql(query)
    checkAnswer(actual, expected)
    scanOf(actual)
  }

  private def scanOf(df: DataFrame): PaimonScan = {
    df.queryExecution.optimizedPlan.collectFirst {
      case relation: DataSourceV2ScanRelation => relation.scan.asInstanceOf[PaimonScan]
    }.get
  }

  private def createFixedBucketTable(hasPrimaryKey: Boolean): Unit = {
    val primaryKeyOption = if (hasPrimaryKey) ", 'primary-key' = 'id'" else ""
    sql(s"""CREATE TABLE T (id INT, payload STRING)
           |TBLPROPERTIES ('bucket' = '4', 'bucket-key' = 'id',
           |'file.format' = 'parquet' $primaryKeyOption)
           |""".stripMargin)
    sql("INSERT INTO T SELECT CAST(id AS INT), concat('v', CAST(id AS STRING)) FROM range(400)")
  }

  private def assertBucketFiles(
      scan: PaimonScan,
      expectedBuckets: Set[Int],
      allSplits: Array[DataSplit]): Unit = {
    val splits = scan.inputSplits.map(_.asInstanceOf[DataSplit])
    assert(splits.map(_.bucket()).toSet == expectedBuckets, scan.description())
    val expectedSplits = allSplits.filter(s => expectedBuckets.contains(s.bucket()))
    assert(dataFileNames(splits) == dataFileNames(expectedSplits), scan.description())
  }

  private def dataFileNames(splits: Array[_ <: DataSplit]): Seq[String] = {
    splits.flatMap(_.dataFiles().asScala.map(_.fileName())).sorted.toSeq
  }

  private def materialize(df: DataFrame): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(df.collect().toSeq), df.schema)
  }
}

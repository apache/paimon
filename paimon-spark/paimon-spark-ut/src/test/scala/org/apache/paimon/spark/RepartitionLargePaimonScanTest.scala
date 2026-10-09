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

package org.apache.paimon.spark

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.spark.catalyst.optimizer.RepartitionLargePaimonScan
import org.apache.paimon.table.InnerTable
import org.apache.paimon.table.source.{DataSplit, Split}
import org.apache.paimon.types.{DataField, DataTypes, RowType}

import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.connector.catalog.{Table => ConnectorTable, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.mockito.Mockito.{mock, when}

import java.util.Collections

import scala.collection.JavaConverters._

/** Rule tests with synthetic scan metadata; no source files or Spark data jobs are needed. */
class RepartitionLargePaimonScanTest extends PaimonSparkTestBase {

  private val enabledKey =
    s"spark.paimon.${SparkConnectorOptions.READ_REPARTITION_LARGE_SCAN_ENABLED.key()}"

  test("disabled by default without planning scan partitions") {
    assert(spark.conf.getOption(enabledKey).isEmpty)
    val scan = relation(Seq(Seq(80L)), failOnPlanning = true)
    assert(RepartitionLargePaimonScan(scan) eq scan)
  }

  test("explicitly disabling the rule avoids planning scan partitions") {
    withSplitConf {
      withSparkSQLConf(enabledKey -> "false") {
        val scan = relation(Seq(Seq(80L)), failOnPlanning = true)
        assert(RepartitionLargePaimonScan(scan) eq scan)
      }
      val enabled = relation(Seq(Seq(80L), Seq(20L)))
      assert(RepartitionLargePaimonScan(enabled).asInstanceOf[Repartition].numPartitions == 2)
    }
  }

  test("partition size includes all splits and exact threshold does not trigger") {
    withSplitConf {
      val fits = relation(Seq(Seq(25L, 25L), Seq(50L)))
      assert(RepartitionLargePaimonScan(fits) eq fits)
      val oversized = relation(Seq(Seq(40L, 40L), Seq(20L)))
      val result = RepartitionLargePaimonScan(oversized).asInstanceOf[Repartition]
      assert(result.numPartitions == 2)
      assert(result.shuffle)
      assert(result.child eq oversized)
      assert(RepartitionLargePaimonScan(result) eq result)
    }
  }

  test("repartition count includes small partitions and rounds up") {
    withSplitConf {
      val result = RepartitionLargePaimonScan(relation(Seq(Seq(80L), Seq(20L), Seq(20L))))
      assert(result.asInstanceOf[Repartition].numPartitions == 3)
    }
  }

  test("minimum partition count controls bytes per core") {
    withSplitConf {
      withSparkSQLConf(
        "spark.sql.files.maxPartitionBytes" -> "1000b",
        "spark.sql.files.minPartitionNum" -> "4") {
        val result = RepartitionLargePaimonScan(relation(Seq(Seq(80L), Seq(20L))))
        // min(1000, max(0, 100 / 4)) = 25 bytes.
        assert(result.asInstanceOf[Repartition].numPartitions == 4)
      }
    }
  }

  test("open file cost contributes to estimation and bounds the threshold") {
    withSplitConf {
      withSparkSQLConf(
        "spark.sql.files.maxPartitionBytes" -> "1000b",
        "spark.sql.files.openCostInBytes" -> "30b",
        "spark.sql.files.minPartitionNum" -> "10") {
        val result = RepartitionLargePaimonScan(relation(Seq(Seq(80L), Seq(20L))))
        // min(1000, max(30, (100 + 2 * 30) / 10)) = 30 bytes.
        // Repartition count uses data bytes, not the artificial file-open cost.
        assert(result.asInstanceOf[Repartition].numPartitions == 4)
      }
    }
  }

  test("Paimon split settings take precedence over Spark file settings") {
    withSplitConf {
      val scan = relation(
        Seq(Seq(80L), Seq(20L)),
        Map("source.split.target-size" -> "25b", "source.split.open-file-cost" -> "0b"))
      assert(RepartitionLargePaimonScan(scan).asInstanceOf[Repartition].numPartitions == 4)
    }
  }

  test("preserve an existing shuffle and leave empty scans unchanged") {
    withSplitConf {
      val existing = Repartition(7, shuffle = true, relation(Seq(Seq(80L))))
      assert(RepartitionLargePaimonScan(existing) eq existing)
      val empty = relation(Seq.empty)
      assert(RepartitionLargePaimonScan(empty) eq empty)
    }
  }

  test("reject a partition count that does not fit Spark's integer limit") {
    withSplitConf {
      withSparkSQLConf("spark.sql.files.maxPartitionBytes" -> "1b") {
        val error = intercept[IllegalArgumentException] {
          RepartitionLargePaimonScan(relation(Seq(Seq(Long.MaxValue))))
        }
        assert(error.getMessage.contains("exceeds"))
      }
    }
  }

  test("rule is registered once in an optimizer batch after scan pushdown") {
    val batches = spark.sessionState.optimizer.batches
    val pushdownIndex = batches.indexWhere(_.name == "Early Filter and Projection Push-Down")
    val lateIndex = batches.indexWhere {
      batch =>
        batch.rules.contains(RepartitionLargePaimonScan) &&
        batch.name == "User Provided Optimizers"
    }
    assert(pushdownIndex >= 0)
    assert(lateIndex > pushdownIndex)
    RepartitionLargePaimonScan.register(spark)
    RepartitionLargePaimonScan.register(spark)
    assert(spark.experimental.extraOptimizations.count(_ == RepartitionLargePaimonScan) == 1)
    withSplitConf {
      val scan = relation(Seq(Seq(80L), Seq(20L)))
      val result = batches(lateIndex).rules.foldLeft(
        scan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan)((plan, rule) => rule(plan))
      assert(result.asInstanceOf[Repartition].numPartitions == 2)
    }
  }

  test("non-Paimon plans are unchanged") {
    withSplitConf {
      val plan = spark.range(10).queryExecution.optimizedPlan
      assert(RepartitionLargePaimonScan(plan) eq plan)
    }
  }

  private def withSplitConf(f: => Unit): Unit = {
    withSparkSQLConf(
      enabledKey -> "true",
      "spark.sql.files.maxPartitionBytes" -> "50b",
      "spark.sql.files.minPartitionNum" -> "1",
      "spark.sql.files.openCostInBytes" -> "0b")(f)
  }

  private def relation(
      partitionFileSizes: Seq[Seq[Long]],
      options: Map[String, String] = Map.empty,
      failOnPlanning: Boolean = false): DataSourceV2ScanRelation = {
    val schema = new StructType().add("a", IntegerType)
    val table = mock(classOf[InnerTable])
    when(table.options()).thenReturn(options.asJava)
    when(table.rowType())
      .thenReturn(new RowType(Collections.singletonList(new DataField(0, "a", DataTypes.INT()))))
    when(table.partitionKeys()).thenReturn(Collections.emptyList[String]())
    when(table.primaryKeys()).thenReturn(Collections.emptyList[String]())

    val partitions = partitionFileSizes.zipWithIndex.map {
      case (fileSizes, partitionIndex) =>
        val splits = fileSizes.zipWithIndex.map {
          case (size, fileIndex) =>
            val file = DataFileMeta.forAppend(
              s"$partitionIndex-$fileIndex.parquet",
              size,
              1,
              null,
              0,
              0,
              1,
              Collections.emptyList[String](),
              null,
              FileSource.APPEND,
              null,
              null,
              null,
              null)
            DataSplit
              .builder()
              .withSnapshot(1)
              .withPartition(BinaryRow.EMPTY_ROW)
              .withBucket(0)
              .withBucketPath("test")
              .withDataFiles(Collections.singletonList(file))
              .rawConvertible(false)
              .build()
        }
        PaimonInputPartition(splits)
    }
    val scan = new PaimonScan(table, schema, Seq.empty, Seq.empty, None, None, None) {
      override protected def getInputSplits: Array[Split] = {
        assert(!failOnPlanning, "Disabled rule must not plan scan splits")
        partitions.flatMap(_.splits).toArray
      }
      override protected def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
        assert(!failOnPlanning, "Disabled rule must not plan scan partitions")
        partitions
      }
    }
    val sparkTable = new ConnectorTable {
      override def name(): String = "test_scan"
      override def schema(): StructType = scan.requiredSchema
      override def capabilities(): java.util.Set[TableCapability] =
        Collections.emptySet[TableCapability]()
    }
    val source = DataSourceV2Relation.create(sparkTable, None, None)
    DataSourceV2ScanRelation(source, scan, source.output)
  }
}

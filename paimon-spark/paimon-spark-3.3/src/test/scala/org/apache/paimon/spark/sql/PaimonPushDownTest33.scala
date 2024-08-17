package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.{PaimonBatch, PaimonInputPartition, PaimonScan, PaimonSparkTestBase, SparkTable}
import org.apache.paimon.table.source.DataSplit
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions

import scala.collection.JavaConverters._

class PaimonPushDownTest33 extends PaimonSparkTestBase {

  import testImplicits._

  test("Paimon pushDown: runtime filter") {
    withTable("source", "t") {
      Seq((1L, "x1", "2023"), (2L, "x2", "2023"), (5L, "x5", "2025"), (6L, "x6", "2026"))
        .toDF("a", "b", "pt t")
        .createOrReplaceTempView("source")

      spark.sql(
        """
          |CREATE TABLE t (id INT, name STRING, `pt t` STRING) PARTITIONED BY (`pt t`)
          |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO t(`id`, `name`, `pt t`) VALUES (1, "a", "2023"), (3, "c", "2023"), (5, "e", "2025"), (7, "g", "2027")
          |""".stripMargin)

      val df1 = spark.sql(
        """
          |SELECT t.id, t.name, source.b FROM source join t
          |ON source.`pt t` = t.`pt t` AND source.`pt t` = '2023'
          |ORDER BY t.id, source.b
          |""".stripMargin)
      val qe1 = df1.queryExecution
      Assertions.assertFalse(qe1.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
//      Assertions.assertTrue(qe1.optimizedPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
//      Assertions.assertTrue(qe1.sparkPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(
        df1,
        Row(1, "a", "x1") :: Row(1, "a", "x2") :: Row(3, "c", "x1") :: Row(3, "c", "x2") :: Nil)

      val df2 = spark.sql(
        """
          |SELECT t.*, source.b FROM source join t
          |ON source.a = t.id AND source.`pt t` = t.`pt t` AND source.a > 3
          |""".stripMargin)
      val qe2 = df1.queryExecution
      Assertions.assertFalse(qe2.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
//      Assertions.assertTrue(qe2.optimizedPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
//      Assertions.assertTrue(qe2.sparkPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(df2, Row(5, "e", "2025", "x5") :: Nil)
    }
  }

}

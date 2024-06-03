package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.spark.sql.Row

class InsertOverwriteTestAskwang extends PaimonSparkTestBase {

  val hasPk = true
  val bucket = 1

  test(s"insert overwrite non-partitioned table: hasPk: $hasPk, bucket: $bucket") {
    val prop = if (hasPk) {
      s"'primary-key'='a,b', 'bucket' = '$bucket' "
    } else if (bucket != -1) {
      s"'bucket-key'='a,b', 'bucket' = '$bucket' "
    } else {
      "'write-only'='true'"
    }

    spark.sql(s"""
                 |CREATE TABLE T (a INT, b INT, c STRING)
                 |TBLPROPERTIES ($prop)
                 |""".stripMargin)

    spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY a, b"),
      Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

    spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')");
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY a, b"),
      Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)
  }

}

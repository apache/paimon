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

package org.apache.paimon.spark.util

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.Row

class ScanPlanHelperTest extends PaimonSparkTestBase with ScanPlanHelper {

  test("ScanPlanHelper: create new scan plan and select with row tracking meta cols") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t VALUES (11, 'a'), (22, 'b')")

      val splits = getPaimonScan("SELECT * FROM t").inputSplits.map(_.asInstanceOf[DataSplit])
      val newScanPlan = createNewScanPlan(splits, createRelationV2("t"))
      val newDf = createDataset(spark, newScanPlan)

      // select original df should not contain meta cols
      checkAnswer(newDf, Seq(Row(11, "a"), Row(22, "b")))

      // select df with row tracking meta cols
      checkAnswer(selectWithRowTracking(newDf), Seq(Row(11, "a", 0, 1), Row(22, "b", 1, 1)))

      // select with row tracking meta cols twice should not add new more meta cols
      checkAnswer(
        selectWithRowTracking(selectWithRowTracking(newDf)),
        Seq(Row(11, "a", 0, 1), Row(22, "b", 1, 1)))

      // select df already contains meta cols with row tracking
      checkAnswer(
        selectWithRowTracking(newDf.select("_ROW_ID", "id")),
        Seq(Row(0, 11, 1), Row(1, 22, 1)))
    }
  }
}

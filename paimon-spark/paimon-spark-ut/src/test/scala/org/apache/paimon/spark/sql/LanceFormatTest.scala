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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class LanceFormatTest extends PaimonSparkTestBase {

  test("Lance format: read and write") {
    withTable("t") {
      sql(
        "CREATE TABLE t (a INT, b STRING, scores ARRAY<DOUBLE>) TBLPROPERTIES ('file.format' = 'lance')")
      sql(
        "INSERT INTO t VALUES (1, 'a', ARRAY(CAST(90.5 as double), CAST(88.0 as double))), (2, 'b', ARRAY(CAST(90.6 as double), CAST(88.1 as double)))")
      checkAnswer(sql("SELECT * FROM t LIMIT 1"), Seq(Row(1, "a", Array(90.5, 88.0))))
      checkAnswer(
        sql("SELECT * FROM t LIMIT 10"),
        Seq(Row(1, "a", Array(90.5, 88.0)), Row(2, "b", Array(90.6, 88.1))))

      assert(
        sql("SELECT file_size_in_bytes FROM `t$files`")
          .collect()
          .map(s => s.get(0).asInstanceOf[Long])
          .apply(0) > 0L)
    }
  }
}

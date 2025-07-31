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

class RowLineageTest extends PaimonSparkTestBase {

  test("Row Lineage: read row lineage") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
      sql("INSERT INTO t VALUES (11, 'a'), (22, 'b')")

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(11, "a", 0, 1), Row(22, "b", 1, 1))
      )
      checkAnswer(
        sql("SELECT _ROW_ID, data, _SEQUENCE_NUMBER, id FROM t"),
        Seq(Row(0, "a", 1, 11), Row(1, "b", 1, 22))
      )
    }
  }
}

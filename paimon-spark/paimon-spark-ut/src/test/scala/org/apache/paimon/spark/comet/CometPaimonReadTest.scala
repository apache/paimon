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

package org.apache.paimon.spark.comet

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class CometPaimonReadTest extends PaimonSparkTestBase {

  test("Paimon Comet Integration: Read simple table") {
    spark.sql(
      s"CREATE TABLE T (id INT, data STRING) USING paimon TBLPROPERTIES ('file.format'='parquet', 'bucket'='-1')")
    spark.sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    // Enable Comet
    spark.sql("ALTER TABLE T SET TBLPROPERTIES ('comet.enabled'='true')")

    val df = spark.sql("SELECT * FROM T ORDER BY id")
    checkAnswer(df, Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
  }
}

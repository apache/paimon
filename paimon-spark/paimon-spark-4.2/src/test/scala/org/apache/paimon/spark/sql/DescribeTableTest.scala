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

import org.apache.spark.sql.Row

class DescribeTableTest extends DescribeTableTestBase {

  // Spark 4.2 hands the partition values over typed, so these match the partition Paimon stored
  // even though the spec is not spelled the way Paimon names it.

  private def partitionValues(spec: String): Seq[Row] =
    spark
      .sql(s"DESCRIBE FORMATTED T PARTITION ($spec)")
      .filter("col_name = 'Partition Values'")
      .select("data_type")
      .collect()
      .toSeq

  test("Paimon describe: describe table partition with a null value") {
    spark.sql("CREATE TABLE T (id INT, p INT) PARTITIONED BY (p)")
    spark.sql("INSERT INTO T VALUES (1, NULL)")
    assert(partitionValues("p = null") == Seq(Row("[p=__DEFAULT_PARTITION__]")))
  }

  test("Paimon describe: describe table partition with a date value and legacy name") {
    // With `partition.legacy-name` (the default) Paimon stores a DATE partition as its epoch day.
    spark.sql("CREATE TABLE T (id INT, dt DATE) PARTITIONED BY (dt)")
    spark.sql("INSERT INTO T VALUES (1, DATE '2021-01-01')")
    assert(partitionValues("dt = '2021-01-01'") == Seq(Row("[dt=18628]")))
  }
}

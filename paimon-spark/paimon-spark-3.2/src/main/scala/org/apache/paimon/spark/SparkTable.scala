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

import org.apache.paimon.table.Table

/** A spark [[org.apache.spark.sql.connector.catalog.Table]] for paimon. */
case class SparkTable(override val table: Table) extends PaimonSparkTableBase(table) {}

/**
 * Per-version companion: Spark 3.2 uses its own simpler `SparkTable` (without V2 row-level ops),
 * which shadows the shaded `paimon-spark-common.SparkTable` at packaging time. `SparkCatalog` in
 * `paimon-spark-common` calls `SparkTable$.MODULE$.of`, and `RowLevelHelper.shouldFallbackToV1`
 * calls `SparkTable$.MODULE$.supportsV2RowLevelOps`, so this companion must expose both methods to
 * avoid `NoSuchMethodError` at runtime. V2 row-level ops require Spark 3.5+, so on 3.2 we always
 * report `false` and every DML goes through Paimon's V1 postHoc fallback path.
 */
object SparkTable {
  def of(table: Table): SparkTable = SparkTable(table)

  private[spark] def supportsV2RowLevelOps(sparkTable: SparkTable): Boolean = false
}

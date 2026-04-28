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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Custom extractor for [[DataSourceV2Relation]]. Spark 4.1 added a 6th `timeTravelSpec` field to
 * `DataSourceV2Relation`, making the default tuple-based pattern matching fail to compile. Using
 * field access keeps call sites compatible with both Spark 3.x (5 fields) and Spark 4.1+ (6
 * fields).
 */
object PaimonV2Relation {
  def unapply(d: DataSourceV2Relation): Option[Table] = Some(d.table)
}

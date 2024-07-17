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

package org.apache.paimon.spark.schema

import org.apache.spark.sql.types.StructType

/** System columns for paimon spark. */
object SparkSystemColumns {

  // for assigning bucket when writing
  val BUCKET_COL = "_bucket_"

  // for row level operation
  val ROW_KIND_COL = "_row_kind_"

  val SPARK_SYSTEM_COLUMNS_NAME: Seq[String] = Seq(BUCKET_COL, ROW_KIND_COL)

  def filterSparkSystemColumns(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(field => SPARK_SYSTEM_COLUMNS_NAME.contains(field.name)))
  }
}

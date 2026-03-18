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

package org.apache.paimon.spark.write

import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.table.Table
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.sources._

import scala.collection.JavaConverters._

abstract class BaseWriteBuilder(table: Table)
  extends WriteBuilder
  with ExpressionHelper
  with SQLConfHelper {

  def partitionRowType(): RowType

  protected def failWithReason(filter: Filter): Unit = {
    throw new RuntimeException(
      s"Only support Overwrite filters with Equal and EqualNullSafe, but got: $filter")
  }

  private def validateFilter(filter: Filter): Unit = filter match {
    case And(left, right) =>
      validateFilter(left)
      validateFilter(right)
    case _: Or => failWithReason(filter)
    case _: Not => failWithReason(filter)
    case e: EqualTo if e.references.length == 1 && !e.value.isInstanceOf[Filter] =>
    case e: EqualNullSafe if e.references.length == 1 && !e.value.isInstanceOf[Filter] =>
    case _: AlwaysTrue | _: AlwaysFalse =>
    case _ => failWithReason(filter)
  }

  // `SupportsOverwrite#canOverwrite` is added since Spark 3.4.0.
  // We do this checking by self to work with previous Spark version.
  protected def failIfCanNotOverwrite(filters: Array[Filter]): Unit = {
    // For now, we only support overwrite with two cases:
    // - overwrite with partition columns to be compatible with v1 insert overwrite
    //   See [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveInsertInto#staticDeleteExpression]].
    // - truncate-like overwrite and the filter is always true.
    //
    // Fast fail for other custom filters which through v2 write interface, e.g.,
    // `dataframe.writeTo(T).overwrite(...)`
    val partitionNames = partitionRowType.getFieldNames.asScala
    val allReferences = filters.flatMap(_.references)
    val containsDataColumn =
      allReferences.exists(reference => !partitionNames.exists(conf.resolver.apply(reference, _)))
    if (containsDataColumn) {
      throw new RuntimeException(
        s"Only support Overwrite filters on partition column ${partitionNames.mkString(", ")}, " +
          s"but got ${filters.mkString(", ")}.")
    }
    if (allReferences.distinct.length < allReferences.length) {
      // fail with `part = 1 and part = 2`
      throw new RuntimeException(
        s"Only support Overwrite with one filter for each partition column, but got ${filters.mkString(", ")}.")
    }
    filters.foreach(validateFilter)
  }
}

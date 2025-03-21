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

import org.apache.paimon.options.Options
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, Not, Or}

import scala.collection.JavaConverters._

abstract class BaseWriteBuilder(table: FileStoreTable, options: Options)
  extends WriteBuilder
  with SQLConfHelper {

  private def failWithReason(filter: Filter): Unit = {
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
    val partitionRowType = table.schema.logicalPartitionType()
    val partitionNames = partitionRowType.getFieldNames.asScala
    val allReferences = filters.flatMap(_.references)
    val containsDataColumn = allReferences.exists {
      reference => !partitionNames.exists(conf.resolver.apply(reference, _))
    }
    if (containsDataColumn) {
      throw new RuntimeException(
        s"Only support Overwrite filters on partition column ${partitionNames.mkString(
            ", ")}, but got ${filters.mkString(", ")}.")
    }
    if (allReferences.distinct.length < allReferences.length) {
      // fail with `part = 1 and part = 2`
      throw new RuntimeException(
        s"Only support Overwrite with one filter for each partition column, but got ${filters.mkString(", ")}.")
    }
    filters.foreach(validateFilter)
  }

  private def parseSaveMode(
      saveMode: SaveMode,
      table: FileStoreTable): (Boolean, Map[String, String]) = {
    var dynamicPartitionOverwriteMode = false
    val overwritePartition = saveMode match {
      case InsertInto => null
      case Overwrite(filter) =>
        if (filter.isEmpty) {
          Map.empty[String, String]
        } else if (isTruncate(filter.get)) {
          Map.empty[String, String]
        } else {
          convertPartitionFilterToMap(filter.get, table.schema.logicalPartitionType())
        }
      case DynamicOverWrite =>
        dynamicPartitionOverwriteMode = true
        Map.empty[String, String]
      case _ =>
        throw new UnsupportedOperationException(s" This mode is unsupported for now.")
    }
    (dynamicPartitionOverwriteMode, overwritePartition)
  }

  /**
   * For the 'INSERT OVERWRITE' semantics of SQL, Spark DataSourceV2 will call the `truncate`
   * methods where the `AlwaysTrue` Filter is used.
   */
  def isTruncate(filter: Filter): Boolean = {
    val filters = splitConjunctiveFilters(filter)
    filters.length == 1 && filters.head.isInstanceOf[AlwaysTrue]
  }

  /** See [[ org.apache.paimon.spark.SparkWriteBuilder#failIfCanNotOverwrite]] */
  def convertPartitionFilterToMap(
      filter: Filter,
      partitionRowType: RowType): Map[String, String] = {
    // todo: replace it with SparkV2FilterConverter when we drop Spark3.2
    val converter = new SparkFilterConverter(partitionRowType)
    splitConjunctiveFilters(filter).map {
      case EqualNullSafe(attribute, value) =>
        (attribute, converter.convertString(attribute, value))
      case EqualTo(attribute, value) =>
        (attribute, converter.convertString(attribute, value))
      case _ =>
        // Should not happen
        throw new RuntimeException(
          s"Only support Overwrite filters with Equal and EqualNullSafe, but got: $filter")
    }.toMap
  }

  private def splitConjunctiveFilters(filter: Filter): Seq[Filter] = {
    filter match {
      case And(filter1, filter2) =>
        splitConjunctiveFilters(filter1) ++ splitConjunctiveFilters(filter2)
      case other => other :: Nil
    }
  }
}

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
package org.apache.paimon.spark.commands

import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.SparkFilterConverter
import org.apache.paimon.table.{BucketMode, FileStoreTable, Table}
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageSerializer}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.sources.{AlwaysTrue, And, EqualNullSafe, Filter, Not, Or}

import java.io.IOException

/** Helper trait for all paimon commands. */
trait PaimonCommand {

  val BUCKET_COL = "_bucket_"

  def getTable: Table

  lazy val bucketMode: BucketMode = getTable match {
    case fileStoreTable: FileStoreTable =>
      fileStoreTable.bucketMode
    case _ =>
      BucketMode.FIXED
  }

  def deserializeCommitMessage(
      serializer: CommitMessageSerializer,
      bytes: Array[Byte]): CommitMessage = {
    try {
      serializer.deserialize(serializer.getVersion, bytes)
    } catch {
      case e: IOException =>
        throw new RuntimeException("Failed to deserialize CommitMessage's object", e)
    }
  }

  /**
   * For the 'INSERT OVERWRITE' semantics of SQL, Spark DataSourceV2 will call the `truncate`
   * methods where the `AlwaysTrue` Filter is used.
   */
  def isTruncate(filter: Filter): Boolean = {
    val filters = splitConjunctiveFilters(filter)
    filters.length == 1 && filters.head.isInstanceOf[AlwaysTrue]
  }

  /**
   * For the 'INSERT OVERWRITE T PARTITION (partitionVal, ...)' semantics of SQL, Spark will
   * transform `partitionVal`s to EqualNullSafe Filters.
   */
  def convertFilterToMap(filter: Filter, partitionRowType: RowType): Map[String, String] = {
    val converter = new SparkFilterConverter(partitionRowType)
    splitConjunctiveFilters(filter).map {
      case EqualNullSafe(attribute, value) =>
        if (isNestedFilterInValue(value)) {
          throw new RuntimeException(
            s"Not support the complex partition value in EqualNullSafe when run `INSERT OVERWRITE`.")
        } else {
          (attribute, converter.convertLiteral(attribute, value).toString)
        }
      case _ =>
        throw new RuntimeException(
          s"Only EqualNullSafe should be used when run `INSERT OVERWRITE`.")
    }.toMap
  }

  def splitConjunctiveFilters(filter: Filter): Seq[Filter] = {
    filter match {
      case And(filter1, filter2) =>
        splitConjunctiveFilters(filter1) ++ splitConjunctiveFilters(filter2)
      case other => other :: Nil
    }
  }

  def isNestedFilterInValue(value: Any): Boolean = {
    value.isInstanceOf[Filter]
  }

}

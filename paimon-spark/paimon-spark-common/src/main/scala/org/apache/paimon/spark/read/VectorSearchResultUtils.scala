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

package org.apache.paimon.spark.read

import org.apache.paimon.globalindex.{GlobalIndexResult, ScoredGlobalIndexResult}
import org.apache.paimon.spark.catalyst.plans.logical.DynamicVectorSearchRelation
import org.apache.paimon.spark.schema.PaimonMetadataColumn

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Utilities for returning vector search results without reading table rows. */
object VectorSearchResultUtils {

  def isVectorSearchMetaOnly(fieldNames: Seq[String]): Boolean = {
    fieldNames.nonEmpty && fieldNames.forall(
      PaimonMetadataColumn.VECTOR_SEARCH_META_COLUMN_NAMES.contains)
  }

  def toRows(result: GlobalIndexResult, requiredSchema: StructType): Array[InternalRow] = {
    val fieldNames = requiredSchema.fieldNames
    val scoreGetter = result match {
      case scored: ScoredGlobalIndexResult => Some(scored.scoreGetter())
      case _ => None
    }
    result
      .results()
      .iterator()
      .asScala
      .map {
        rowId =>
          new GenericInternalRow(
            fieldNames.map(valueOf(_, rowId, scoreGetter)).asInstanceOf[Array[Any]])
      }
      .toArray
  }

  def valueOf(
      fieldName: String,
      rowId: Long,
      scoreGetter: Option[org.apache.paimon.globalindex.ScoreGetter]): Any = {
    fieldName match {
      case PaimonMetadataColumn.ROW_ID_COLUMN => rowId
      case PaimonMetadataColumn.SEARCH_SCORE_COLUMN =>
        scoreGetter.map(_.score(rowId)).getOrElse(Float.NaN)
      case _ =>
        throw new IllegalArgumentException(
          s"Field $fieldName cannot be returned directly from vector search result.")
    }
  }
}

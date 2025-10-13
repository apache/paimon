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

import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.types.RowType

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.sources.{Filter, In}

import java.util.List

import scala.collection.JavaConverters._

/** Base trait for Paimon scan that supports runtime filtering. */
trait PaimonBaseSupportsRuntimeFiltering extends SupportsRuntimeFiltering {

  protected var partitionKeys: List[String]
  protected var rowType: RowType
  protected var readBuilder: ReadBuilder
  protected var inputPartitions: Seq[PaimonInputPartition]

  override def filterAttributes(): Array[NamedReference] = {
    val requiredFields = readBuilder.readType().getFieldNames.asScala
    partitionKeys.asScala.toArray
      .filter(requiredFields.contains)
      .map(fieldReference)
  }

  override def filter(filters: Array[Filter]): Unit = {
    val converter = new SparkFilterConverter(rowType)
    val partitionFilter = filters.flatMap {
      case in @ In(attr, _) if partitionKeys.contains(attr) =>
        Some(converter.convert(in))
      case _ => None
    }
    if (partitionFilter.nonEmpty) {
      readBuilder.withFilter(partitionFilter.head)
      // set inputPartitions null to trigger to get the new splits.
      inputPartitions = null
    }
  }
}

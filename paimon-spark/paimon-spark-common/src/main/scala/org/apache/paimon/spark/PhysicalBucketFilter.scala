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

import org.apache.paimon.data.GenericRow
import org.apache.paimon.predicate.{Equal, LeafPredicate, Predicate, PredicateBuilder}
import org.apache.paimon.spark.schema.PaimonMetadataColumn.BUCKET_COLUMN
import org.apache.paimon.table.{FileStoreTable, Table}
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.types.{DataType, DataTypes, RowType}
import org.apache.paimon.utils.Filter

import scala.collection.JavaConverters._

/** Evaluates metadata predicates on physical bucket IDs, independently of the current bucket count. */
private[spark] case class PhysicalBucketFilter(predicate: Predicate)
  extends Filter[Integer]
  with Serializable {

  override def test(bucket: Integer): Boolean = predicate.test(GenericRow.of(bucket))
}

private[spark] object PhysicalBucketFilter {

  val rowType: RowType =
    RowType.of(Array[DataType](DataTypes.INT()), Array(BUCKET_COLUMN))

  def supports(table: Table): Boolean = {
    table.isInstanceOf[FileStoreTable] &&
    !table.rowType().containsField(BUCKET_COLUMN) &&
    !PostponeMergeOnRead.usesCustomSource(table)
  }

  def push(readBuilder: ReadBuilder, predicates: Seq[Predicate], scanBucket: Option[Int]): Unit = {
    if (predicates.nonEmpty) {
      val predicate = PredicateBuilder.and(predicates.asJava)
      // A plain equality can also use the existing single-bucket manifest pruning.
      PredicateBuilder
        .splitAnd(predicate)
        .asScala
        .collectFirst {
          case leaf: LeafPredicate
              if leaf.function() == Equal.INSTANCE && leaf.fieldRefOptional().isPresent &&
                leaf.literals().get(0).isInstanceOf[Integer] =>
            leaf.literals().get(0).asInstanceOf[Integer].intValue()
        }
        // Preserve scan.bucket when it conflicts with the equality; the full filter below
        // intersects with the configured bucket and produces an empty scan.
        .filter(bucket => scanBucket.forall(_ == bucket))
        .foreach(readBuilder.withBucket)
      readBuilder.withBucketFilter(PhysicalBucketFilter(predicate))
    }
  }
}

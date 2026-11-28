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

package org.apache.spark.sql.execution.shim

import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.catalog.SparkBaseCatalog

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.execution.PaimonTableAsSelectHelper.{canAtomicReplace, checkNonAtomicSelfReference, supportsCatalog}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Strategy, PaimonTableAsSelectWriteExec}

import scala.collection.JavaConverters._

case class PaimonReplaceTableAsSelectStrategy(spark: SparkSession) extends Strategy {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  // Keep Spark's replacement planning and prepare the write after the target table is created.
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case ReplaceTableAsSelect(
            ResolvedDBObjectName(catalog: SparkBaseCatalog, ident),
            parts,
            query,
            tableSpec: TableSpec,
            _,
            _) if supportsCatalog(catalog, tableSpec.provider.orNull) =>
        val properties = CatalogV2Util.convertTableProperties(tableSpec)
        catalog match {
          case paimon: SparkCatalog =>
            paimon.checkPartitionedFormatTableCtas(
              ident.asIdentifier,
              tableSpec.provider.orNull,
              parts.nonEmpty,
              properties.asJava)
          case _ =>
        }
        // A replace that cannot happen in place drops the old table before the query reads it.
        if (!canAtomicReplace(catalog, ident.asIdentifier, tableSpec.provider, properties, parts)) {
          checkNonAtomicSelfReference(catalog, ident.asIdentifier, query)
        }
        return new DataSourceV2Strategy(spark)
          .apply(plan)
          .map(exec => PaimonTableAsSelectWriteExec.prepare(exec))
      case _ =>
    }
    Nil
  }
}

case class PaimonReplaceTableStrategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Nil
}

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

import org.apache.paimon.CoreOptions

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, StagingTableCatalog}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.CreateTableAsSelectExec
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class PaimonCreateTableAsSelectStrategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists) =>
      catalog match {
        case _: StagingTableCatalog =>
          throw new RuntimeException("Paimon can't extend StagingTableCatalog for now.")
        case _ =>
          val coreOptionKeys = CoreOptions.getOptions.asScala.map(_.key()).toSeq
          val (coreOptions, writeOptions) = options.partition {
            case (key, _) => coreOptionKeys.contains(key)
          }
          val newProps = CatalogV2Util.withDefaultOwnership(props) ++ coreOptions
          CreateTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            newProps,
            new CaseInsensitiveStringMap(writeOptions.asJava),
            ifNotExists
          ) :: Nil
      }
    case _ => Nil
  }
}

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

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

case class PaimonReplaceTableAsSelectStrategy(spark: SparkSession) extends Strategy {

  // Spark plans the replace itself, which drops the old table before it writes, so reject what
  // it cannot write before that. The check uses the properties Spark creates the table with.
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case ReplaceTableAsSelect(catalog: SparkCatalog, ident, parts, _, props, _, _) =>
        val properties = CatalogV2Util.withDefaultOwnership(props)
        catalog.checkPartitionedFormatTableCtas(
          ident,
          properties.get(TableCatalog.PROP_PROVIDER).orNull,
          parts.nonEmpty,
          properties.asJava)
      case _ =>
    }
    Nil
  }
}

case class PaimonReplaceTableStrategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Nil
}

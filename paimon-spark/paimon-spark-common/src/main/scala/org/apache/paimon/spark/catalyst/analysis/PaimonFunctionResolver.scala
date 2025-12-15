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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.catalog.SupportV1Function

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.extensions.UnResolvedPaimonV1Function
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_FUNCTION

case class PaimonFunctionResolver(spark: SparkSession) extends Rule[LogicalPlan] {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
      case l: LogicalPlan =>
        l.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
          case u: UnResolvedPaimonV1Function if u.arguments.forall(_.resolved) =>
            u.funcIdent.catalog match {
              case Some(catalog) =>
                catalogManager.catalog(catalog) match {
                  case v1FunctionCatalog: SupportV1Function =>
                    v1FunctionCatalog.registerAndResolveV1Function(u)
                  case _ =>
                    throw new IllegalArgumentException(
                      s"Catalog $catalog is not a v1 function catalog")
                }
              case None => throw new IllegalArgumentException("Catalog name is not defined")
            }
        }
    }
}

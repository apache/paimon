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

package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableLike, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.HiveSerDe

/**
 * Spark 4.2 parses CREATE TABLE LIKE into the v2 [[CreateTableLike]] plan instead of
 * `CreateTableLikeCommand`, so [[RewriteCreateTableLikeCommand]] never sees it. This rule hands the
 * same fields to [[RewriteCreateTableLikeCommand.rewrite]].
 */
case class Spark42RewriteCreateTableLike(spark: SparkSession) extends Rule[LogicalPlan] {

  private lazy val rewriter = RewriteCreateTableLikeCommand(spark)

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case c @ CreateTableLike(
          UnresolvedIdentifier(target, _),
          source: UnresolvedTableOrView,
          location,
          provider,
          serdeInfo,
          properties,
          ifNotExists) =>
      rewriter
        .rewrite(
          target,
          source.multipartIdentifier,
          provider,
          // Spark builds the same storage format for its v1 fallback.
          HiveSerDe.buildStorageFormat(
            location,
            serdeInfo,
            QueryCompilationErrors.invalidFileFormatForStoredAsError),
          properties,
          ifNotExists
        )
        .getOrElse(c)
  }
}

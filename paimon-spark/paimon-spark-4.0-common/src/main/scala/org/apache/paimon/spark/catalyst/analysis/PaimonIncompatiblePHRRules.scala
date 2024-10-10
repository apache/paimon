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

import org.apache.paimon.spark.commands.PaimonTruncateTableCommand

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TruncatePartition}
import org.apache.spark.sql.catalyst.rules.Rule

/** These post-hoc resolution rules are incompatible between different versions of spark. */
case class PaimonIncompatiblePHRRules(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case t @ TruncatePartition(PaimonRelation(table), ResolvedPartitionSpec(names, ident, _))
          if t.resolved =>
        assert(names.length == ident.numFields, "Names and values of partition don't match")
        val resolver = session.sessionState.conf.resolver
        val schema = table.schema
        val partitionSpec = names.zipWithIndex.map {
          case (name, index) =>
            val field = schema.find(f => resolver(f.name, name)).getOrElse {
              throw new RuntimeException(s"$name is not a valid partition column in $schema.")
            }
            (name -> ident.get(index, field.dataType).toString)
        }.toMap
        PaimonTruncateTableCommand(table, partitionSpec)

      case _ => plan
    }
  }

}

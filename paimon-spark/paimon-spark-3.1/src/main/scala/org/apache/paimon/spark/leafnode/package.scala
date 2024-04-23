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

import org.apache.paimon.spark.catalyst.trees.PaimonLeafLike

import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, ParsedStatement}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

package object leafnode {
  trait PaimonLeafParsedStatement extends ParsedStatement with PaimonLeafLike[LogicalPlan]

  trait PaimonLeafRunnableCommand extends RunnableCommand with PaimonLeafLike[LogicalPlan]

  trait PaimonLeafCommand extends Command with PaimonLeafLike[LogicalPlan]

  trait PaimonLeafV2CommandExec extends V2CommandExec with PaimonLeafLike[SparkPlan]

}

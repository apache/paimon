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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateAction}

/**
 * Custom extractor for [[UpdateAction]] that only surfaces the fields Paimon cares about. Spark 4.1
 * added a third `fromStar` field to `UpdateAction`, which breaks the default 2-tuple pattern
 * destructuring used across paimon-spark-common. Using field access keeps the call sites compatible
 * with both Spark 3.x (2 fields) and Spark 4.1+ (3 fields).
 */
object PaimonUpdateAction {
  def unapply(a: UpdateAction): Option[(Option[Expression], Seq[Assignment])] =
    Some((a.condition, a.assignments))
}

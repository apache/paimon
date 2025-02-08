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

package org.apache.paimon.spark.sql

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter

class PaimonPushDownTest extends PaimonPushDownTestBase {

  override def checkFilterExists(sql: String): Boolean = {
    spark
      .sql(sql)
      .queryExecution
      .optimizedPlan
      .find {
        case Filter(_: Expression, _) => true
        case _ => false
      }
      .isDefined
  }

  override def checkEqualToFilterExists(sql: String, name: String, value: Literal): Boolean = {
    spark
      .sql(sql)
      .queryExecution
      .optimizedPlan
      .find {
        case Filter(c: Expression, _) =>
          c.find {
            case EqualTo(a: AttributeReference, r: Literal) =>
              a.name.equals(name) && r.equals(value)
            case _ => false
          }.isDefined
        case _ => false
      }
      .isDefined
  }
}

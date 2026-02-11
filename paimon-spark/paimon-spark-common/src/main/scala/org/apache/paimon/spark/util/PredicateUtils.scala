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

package org.apache.paimon.spark.util

import org.apache.paimon.predicate.{Between, CompareUtils, GreaterOrEqual, LeafPredicate, LessOrEqual, Predicate}

import java.util
import java.util.Objects

object PredicateUtils {

  /** Try to convert AND's children predicates to BETWEEN leaf predicate, return None if failed. */
  def convertToBetweenFunction(
      leftPredicate: Predicate,
      rightPredicate: Predicate): Option[Predicate] = {

    def toBetweenLeafPredicate(
        gtePredicate: LeafPredicate,
        ltePredicate: LeafPredicate): Option[Predicate] = {
      // gtePredicate and ltePredicate should have the same transform
      val transform = gtePredicate.transform()
      val literalLb = gtePredicate.literals().get(0)
      val literalUb = ltePredicate.literals().get(0)
      if (CompareUtils.compareLiteral(transform.outputType(), literalLb, literalUb) > 0) {
        None
      } else {
        Some(
          new LeafPredicate(transform, Between.INSTANCE, util.Arrays.asList(literalLb, literalUb)))
      }
    }

    (leftPredicate, rightPredicate) match {
      case (left: LeafPredicate, right: LeafPredicate) =>
        if (!Objects.equals(left.transform(), right.transform())) {
          return None
        }
        (left.function(), right.function()) match {
          case (_: GreaterOrEqual, _: LessOrEqual) =>
            toBetweenLeafPredicate(left, right)
          case (_: LessOrEqual, _: GreaterOrEqual) =>
            toBetweenLeafPredicate(right, left)
          case _ => None
        }
      case _ =>
        None
    }
  }
}

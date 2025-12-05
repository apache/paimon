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

package org.apache.paimon.spark.aggregate

import org.apache.paimon.data.BinaryString
import org.apache.paimon.predicate.CompareUtils
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.stats.SimpleStatsEvolutions
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.DataField

import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.unsafe.types.UTF8String

trait AggFuncEvaluator[T] {
  def update(dataSplit: DataSplit): Unit

  def result(): T

  def resultType: DataType

  def prettyName: String
}

class CountStarEvaluator extends AggFuncEvaluator[Long] {
  private var _result: Long = 0L

  override def update(dataSplit: DataSplit): Unit = {
    _result += dataSplit.mergedRowCount()
  }

  val a: Int = 1;
  override def result(): Long = _result

  override def resultType: DataType = LongType

  override def prettyName: String = "count_star"
}

case class MinEvaluator(idx: Int, dataField: DataField, evolutions: SimpleStatsEvolutions)
  extends AggFuncEvaluator[Any] {
  private var _result: Any = _

  override def update(dataSplit: DataSplit): Unit = {
    val other = dataSplit.minValue(idx, dataField, evolutions)
    if (other == null) {
      return
    }
    if (_result == null || CompareUtils.compareLiteral(dataField.`type`(), _result, other) > 0) {
      _result = other;
    }
  }

  override def result(): Any = _result match {
    case s: BinaryString => UTF8String.fromString(s.toString)
    case a => a
  }

  override def resultType: DataType = SparkTypeUtils.fromPaimonType(dataField.`type`())

  override def prettyName: String = "min"
}

case class MaxEvaluator(idx: Int, dataField: DataField, evolutions: SimpleStatsEvolutions)
  extends AggFuncEvaluator[Any] {
  private var _result: Any = _

  override def update(dataSplit: DataSplit): Unit = {
    val other = dataSplit.maxValue(idx, dataField, evolutions)
    if (other == null) {
      return
    }
    if (_result == null || CompareUtils.compareLiteral(dataField.`type`(), _result, other) < 0) {
      _result = other
    }
  }

  override def result(): Any = _result match {
    case s: BinaryString => UTF8String.fromString(s.toString)
    case a => a
  }

  override def resultType: DataType = SparkTypeUtils.fromPaimonType(dataField.`type`())

  override def prettyName: String = "max"
}

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

package org.apache.paimon.spark.read

import org.apache.paimon.table.source.Split
import org.apache.paimon.types.{DataTypes, RowType}

import org.scalatest.funsuite.AnyFunSuite

import java.util.{Optional, OptionalLong}

class PaimonStatisticsTest extends AnyFunSuite {

  private val rowType = RowType.of(DataTypes.INT())

  test("non-positive split row counts are unknown") {
    Seq(Seq(0L), Seq(-2L), Seq(5L, 0L), Seq(5L, -2L)).foreach {
      rowCounts => assert(!statistics(rowCounts: _*).numRows.isPresent, rowCounts.toString())
    }

    assert(statistics(2L, 3L).numRows.getAsLong == 5L)
  }

  test("non-positive scan row counts are unknown for a non-empty scan") {
    Seq(0L, -1L, -2L).foreach {
      rowCount =>
        val result = statistics(OptionalLong.of(rowCount), -1L)
        assert(!result.numRows.isPresent, rowCount.toString)
    }
  }

  test("empty scans have an exact zero row count") {
    assert(statistics().numRows.getAsLong == 0L)
    assert(statistics(OptionalLong.of(0L)).numRows.getAsLong == 0L)
  }

  test("positive scan row count takes priority over split sentinels") {
    assert(statistics(OptionalLong.of(4L), -1L).numRows.getAsLong == 4L)
  }

  test("overflowed split row count is unknown") {
    assert(!statistics(Long.MaxValue, 1L).numRows.isPresent)
  }

  private def statistics(rowCounts: Long*): PaimonStatistics =
    statistics(OptionalLong.empty(), rowCounts: _*)

  private def statistics(scanRowCount: OptionalLong, rowCounts: Long*): PaimonStatistics = {
    val splits = rowCounts.map {
      count =>
        new Split {
          override def rowCount(): Long = count

          override def mergedRowCount(): OptionalLong = OptionalLong.empty()
        }
    }.toArray
    PaimonStatistics(splits, rowType, rowType, Optional.empty(), scanRowCount)
  }
}

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

package org.apache.paimon.spark.sources

import org.apache.paimon.utils.JsonSerdeUtil

import org.scalatest.funsuite.AnyFunSuite

class PaimonSourceOffsetTest extends AnyFunSuite {

  test("round trip total splits in offset JSON") {
    val offset: PaimonSourceOffset = PaimonSourceOffset.withTotalSplits(
      snapshotId = 3L,
      index = 1L,
      scanSnapshot = true,
      totalSplits = 2L)

    val restored = PaimonSourceOffset(offset.json())

    assert(restored.snapshotId == 3L)
    assert(restored.index == 1L)
    assert(restored.scanSnapshot)
    assert(restored.totalSplits.contains(2L))
  }

  test("copy and Java serialization preserve total splits") {
    val offset = PaimonSourceOffset.withTotalSplits(
      snapshotId = 3L,
      index = 1L,
      scanSnapshot = true,
      totalSplits = 2L)

    val copied = offset.copy(index = 0L)
    val deserialized = org.apache.paimon.utils.InstantiationUtil.clone(offset)

    assert(copied.index == 0L)
    assert(copied.totalSplits.contains(2L))
    assert(!copied.snapshotCompleted)
    assert(deserialized.totalSplits.contains(2L))
    assert(deserialized.snapshotCompleted)
  }

  test("copy clears total splits when snapshot identity changes") {
    val offset = PaimonSourceOffset.withTotalSplits(
      snapshotId = 3L,
      index = 1L,
      scanSnapshot = true,
      totalSplits = 2L)

    assert(offset.copy(snapshotId = 4L).totalSplits.isEmpty)
    assert(offset.copy(scanSnapshot = false).totalSplits.isEmpty)
  }

  test("read legacy offset JSON without total splits") {
    val json = """{"snapshotId":3,"index":1,"scanSnapshot":false}"""

    val restored = PaimonSourceOffset(json)

    assert(restored.totalSplits.isEmpty)
    assert(!JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(restored.json()).has("totalSplits"))
  }

  test("new offset JSON remains readable by the legacy decoder") {
    val offset = PaimonSourceOffset.withTotalSplits(
      snapshotId = 3L,
      index = 1L,
      scanSnapshot = false,
      totalSplits = 2L)

    val restoredByLegacyDecoder = legacyRead(offset.json())

    assert(restoredByLegacyDecoder.snapshotId == 3L)
    assert(restoredByLegacyDecoder.index == 1L)
    assert(!restoredByLegacyDecoder.scanSnapshot)
  }

  test("total splits does not change the three-field case class API") {
    val offset: PaimonSourceOffset = PaimonSourceOffset.withTotalSplits(
      snapshotId = 3L,
      index = 1L,
      scanSnapshot = false,
      totalSplits = 2L)

    assert(offset.productArity == 3)
    val PaimonSourceOffset(snapshotId, index, scanSnapshot) = offset
    assert(snapshotId == 3L)
    assert(index == 1L)
    assert(!scanSnapshot)

    classOf[PaimonSourceOffset].getConstructor(
      java.lang.Long.TYPE,
      java.lang.Long.TYPE,
      java.lang.Boolean.TYPE)
    classOf[PaimonSourceOffset].getMethod(
      "copy",
      java.lang.Long.TYPE,
      java.lang.Long.TYPE,
      java.lang.Boolean.TYPE)
    PaimonSourceOffset.getClass.getMethod(
      "apply",
      java.lang.Long.TYPE,
      java.lang.Long.TYPE,
      java.lang.Boolean.TYPE)
  }

  test("indexed data split keeps its three-field case class API") {
    val split = IndexedDataSplit(3L, 1L, null)

    assert(split.productArity == 3)
    val IndexedDataSplit(snapshotId, index, entry) = split
    assert(snapshotId == 3L)
    assert(index == 1L)
    assert(entry == null)

    classOf[IndexedDataSplit].getConstructor(
      java.lang.Long.TYPE,
      java.lang.Long.TYPE,
      classOf[org.apache.paimon.table.source.DataSplit])
    assert(IndexedDataSplit.isInstanceOf[Function3[_, _, _, _]])
    IndexedDataSplit.getClass.getMethod("tupled")
    IndexedDataSplit.getClass.getMethod("curried")
  }

  private def legacyRead(json: String): PaimonSourceOffset = {
    val node = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(json)
    PaimonSourceOffset(
      node.get("snapshotId").asLong(),
      node.get("index").asLong(),
      node.get("scanSnapshot").asBoolean())
  }
}

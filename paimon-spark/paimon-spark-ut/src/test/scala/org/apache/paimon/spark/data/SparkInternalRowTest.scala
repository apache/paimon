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

package org.apache.paimon.spark.data

import org.apache.paimon.types.{BinaryType, DataField, DataTypeRoot, RowType}

import org.apache.spark.SparkFunSuite

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

class SparkInternalRowTest extends SparkFunSuite {

  test("cache blob field lookup for reused row type") {
    val getTypeRootCount = new AtomicInteger()
    val rowType = newRowType(getTypeRootCount)

    SparkInternalRow.create(rowType)
    SparkInternalRow.create(rowType)

    assert(getTypeRootCount.get() == 10)
  }

  private def newRowType(getTypeRootCount: AtomicInteger): RowType = {
    new RowType(
      (0 until 10)
        .map(i => new DataField(i, s"f$i", new CountingBinaryType(getTypeRootCount)))
        .asJava)
  }

  private class CountingBinaryType(getTypeRootCount: AtomicInteger) extends BinaryType {

    override def getTypeRoot: DataTypeRoot = {
      getTypeRootCount.incrementAndGet()
      super.getTypeRoot
    }
  }
}

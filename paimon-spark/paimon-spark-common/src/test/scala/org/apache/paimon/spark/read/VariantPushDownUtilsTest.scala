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

import org.apache.paimon.types.DataTypes

import org.scalatest.funsuite.AnyFunSuite

/** Tests for {@link VariantPushDownUtils}. */
class VariantPushDownUtilsTest extends AnyFunSuite {

  private def extraction(path: String) =
    (Seq("v"), VariantExtractionInfo(DataTypes.STRING(), path, false, "UTC"), false)

  test("accept an extraction path that can be encoded") {
    val (byPath, accepted) = VariantPushDownUtils.acceptByPath(IndexedSeq(extraction("$.a.b")))
    assert(accepted === Array(true))
    assert(byPath.keySet === Set(Seq("v")))
  }

  test("reject an extraction path containing the metadata delimiter") {
    Seq("$.a;b", "$[\"a;b\"]").foreach {
      path =>
        val (byPath, accepted) = VariantPushDownUtils.acceptByPath(IndexedSeq(extraction(path)))
        assert(accepted === Array(false))
        assert(byPath.isEmpty)
    }
  }
}

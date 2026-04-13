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

package org.apache.paimon.spark.procedure

import org.scalactic.source.Position
import org.scalatest.Tag

/**
 * Spark 4.1 version of CompactProcedureTest.
 *
 * In Spark 4.1.1, MemoryStream was relocated from
 * org.apache.spark.sql.execution.streaming.MemoryStream to
 * org.apache.spark.sql.execution.streaming.runtime.MemoryStream. CompactProcedureTestBase in
 * paimon-spark-ut was compiled against Spark 4.0.2 and its bytecode references the old package
 * path, causing NoClassDefFoundError at runtime. Tests that use MemoryStream are excluded here.
 */
class CompactProcedureTest extends CompactProcedureTestBase {

  // Tests that use MemoryStream (relocated in Spark 4.1.1) are excluded to prevent
  // NoClassDefFoundError from aborting the entire test suite.
  // Must be a def (not val) because test() is called during parent constructor init,
  // before subclass fields are initialized.
  private def streamingTests: Set[String] = Set(
    "Paimon Procedure: sort compact",
    "Paimon Procedure: sort compact with partition",
    "Paimon Procedure: compact for pk",
    "Paimon Procedure: cluster for unpartitioned table",
    "Paimon Procedure: cluster for partitioned table",
    "Paimon Procedure: cluster with deletion vectors"
  )

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (streamingTests.contains(testName)) {
      super.ignore(testName, testTags: _*)(testFun)
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }
}

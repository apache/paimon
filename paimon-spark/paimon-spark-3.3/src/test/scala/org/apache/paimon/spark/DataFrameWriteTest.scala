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

import org.junit.jupiter.api.Assertions

class DataFrameWriteTest extends PaimonSparkTestBase {

  test("Paimon: DataFrameWrite.saveAsTable") {

    import testImplicits._

    Seq((1L, "x1"), (2L, "x2"))
      .toDF("a", "b")
      .write
      .format("paimon")
      .mode("append")
      .option("primary-key", "a")
      .option("bucket", "-1")
      .option("target-file-size", "256MB")
      .option("write.merge-schema", "true")
      .option("write.merge-schema.explicit-cast", "true")
      .saveAsTable("test_ctas")

    val paimonTable = loadTable("test_ctas")
    Assertions.assertEquals(1, paimonTable.primaryKeys().size())
    Assertions.assertEquals("a", paimonTable.primaryKeys().get(0))

    // check all the core options
    Assertions.assertEquals("-1", paimonTable.options().get("bucket"))
    Assertions.assertEquals("256MB", paimonTable.options().get("target-file-size"))

    // non-core options should not be here.
    Assertions.assertFalse(paimonTable.options().containsKey("write.merge-schema"))
    Assertions.assertFalse(paimonTable.options().containsKey("write.merge-schema.explicit-cast"))
  }

}

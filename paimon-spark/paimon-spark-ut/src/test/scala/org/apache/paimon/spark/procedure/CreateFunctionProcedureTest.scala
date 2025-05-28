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

import org.apache.paimon.spark.PaimonRestCatalogSparkTestBase

import org.apache.spark.sql.Row

/** Test for [[CreateFunctionProcedure]]. */
class CreateFunctionProcedureTest extends PaimonRestCatalogSparkTestBase {
  test(s"test create function procedure") {
    checkAnswer(
      spark.sql(
        "CALL sys.create_function('function_test', '[{\"id\": 0, \"name\":\"length\", \"type\":\"INT\"}, {\"id\": 1, \"name\":\"width\", \"type\":\"INT\"}]', '[{\"id\": 0, \"name\":\"area\", \"type\":\"BIGINT\"}]')"),
      Row(true)
    );
  }
}

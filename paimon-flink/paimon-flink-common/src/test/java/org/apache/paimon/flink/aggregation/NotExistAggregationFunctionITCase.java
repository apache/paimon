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

package org.apache.paimon.flink.aggregation;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for testing the aggregation merge engine with not exist aggregation function. */
public class NotExistAggregationFunctionITCase extends CatalogITCaseBase {

    @Test
    public void testNotExistAggregationFunction() {
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE test_not_exist("
                                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                                + "  f0 INT"
                                                + ") WITH ("
                                                + "  'merge-engine' = 'aggregation',"
                                                + "  'fields.f0.aggregate-function' = 'not_exist'"
                                                + ")"))
                .rootCause()
                .hasMessageContaining("Could not find any factory for identifier 'not_exist'");
    }
}

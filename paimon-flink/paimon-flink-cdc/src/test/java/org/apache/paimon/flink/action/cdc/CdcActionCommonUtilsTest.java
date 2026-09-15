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

package org.apache.paimon.flink.action.cdc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcActionCommonUtilsTest {

    @Test
    void testParseTableConfigByTable() {
        Map<String, Map<String, String>> result =
                CdcActionCommonUtils.parseTableConfigByTable(
                        Arrays.asList("orders:bucket=8", "orders:merge-engine=deduplicate"));

        Map<String, String> expected = new HashMap<>();
        expected.put("bucket", "8");
        expected.put("merge-engine", "deduplicate");
        assertThat(result).containsEntry("orders", expected);
    }

    @Test
    void testRejectDuplicateTableConfig() {
        assertThatThrownBy(
                        () ->
                                CdcActionCommonUtils.parseTableConfigByTable(
                                        Arrays.asList("orders:bucket=8", "orders:bucket=4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate table configuration");
    }

    void testRejectSinkConfiguration() {
        assertThatThrownBy(
                        () ->
                                CdcActionCommonUtils.parseTableConfigByTable(
                                        Arrays.asList("orders:sink.parallelism=1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be configured per table");
    }
}

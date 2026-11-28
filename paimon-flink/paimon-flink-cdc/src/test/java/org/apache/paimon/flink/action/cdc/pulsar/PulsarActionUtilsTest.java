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

package org.apache.paimon.flink.action.cdc.pulsar;

import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.firstMatchingTopic;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PulsarActionUtils}. */
class PulsarActionUtilsTest {

    private static final Pattern PATTERN = Pattern.compile("public/default/orders_[0-9]+");

    @Test
    void testFirstMatchingTopicFiltersUnfilteredResult() {
        GetTopicsResult result =
                new GetTopicsResult(
                        Arrays.asList(
                                "persistent://public/default/audit_log",
                                "persistent://public/default/__change_events",
                                "persistent://public/default/orders_1"),
                        null,
                        false,
                        true);

        assertThat(firstMatchingTopic(result, PATTERN))
                .isEqualTo("persistent://public/default/orders_1");
    }

    @Test
    void testFirstMatchingTopicWithoutMatch() {
        GetTopicsResult result =
                new GetTopicsResult(
                        Arrays.asList("persistent://public/default/audit_log"), null, false, true);

        assertThat(firstMatchingTopic(result, PATTERN)).isNull();
    }

    @Test
    void testFirstMatchingTopicSkipsInternalTopics() {
        GetTopicsResult result =
                new GetTopicsResult(
                        Arrays.asList(
                                "persistent://public/default/__change_events",
                                "persistent://public/default/orders_2"),
                        null,
                        true,
                        true);

        assertThat(firstMatchingTopic(result, Pattern.compile("public/default/orders.*")))
                .isEqualTo("persistent://public/default/orders_2");
    }
}

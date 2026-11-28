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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionPredicateHelper}. */
public class PartitionPredicateHelperTest {

    @Test
    public void testParseSinglePartition() {
        List<String> keys = Collections.singletonList("pt");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{1}", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt")).isEqualTo("1");
    }

    @Test
    public void testParseValueContainingCommaSpace() {
        // Partition value "hello, world" contains ", " which is the split delimiter.
        List<String> keys = Collections.singletonList("pt");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{hello, world}", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt")).isEqualTo("hello, world");
    }

    @Test
    public void testParseMultiPartition() {
        List<String> keys = Arrays.asList("pt1", "pt2");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{a, b}", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt1")).isEqualTo("a");
        assertThat(result.get("pt2")).isEqualTo("b");
    }

    @Test
    public void testParseMultiPartitionLastValueContainsComma() {
        List<String> keys = Arrays.asList("pt1", "pt2");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{a, hello, world}", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt1")).isEqualTo("a");
        assertThat(result.get("pt2")).isEqualTo("hello, world");
    }

    @Test
    public void testParseEmptyPartitionKeys() {
        List<String> keys = Collections.emptyList();
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{a}", keys);
        assertThat(result).isNull();
    }

    @Test
    public void testParseWithoutBraces() {
        List<String> keys = Arrays.asList("pt1", "pt2");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("a, b", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt1")).isEqualTo("a");
        assertThat(result.get("pt2")).isEqualTo("b");
    }

    @Test
    public void testParseEmptyStringValue() {
        List<String> keys = Collections.singletonList("pt");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{}", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt")).isEqualTo("");
    }

    @Test
    public void testParseTrailingEmptyComponent() {
        // Without a split limit, trailing empty strings are discarded and "{a, }"
        // parses as a single field, failing the key-count check.
        List<String> keys = Arrays.asList("pt1", "pt2");
        LinkedHashMap<String, String> result =
                PartitionPredicateHelper.parsePartitionSpec("{a, }", keys);
        assertThat(result).isNotNull();
        assertThat(result.get("pt1")).isEqualTo("a");
        assertThat(result.get("pt2")).isEqualTo("");
    }
}

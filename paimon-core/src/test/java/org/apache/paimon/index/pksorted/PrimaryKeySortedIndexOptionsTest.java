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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for primary-key sorted index options in {@link CoreOptions}. */
class PrimaryKeySortedIndexOptionsTest {

    @Test
    void testResolvesBTreeIndexColumns() {
        CoreOptions options =
                new CoreOptions(Collections.singletonMap("pk-btree.index.columns", " name,  age "));

        assertThat(options.primaryKeyBTreeIndexColumns()).containsExactly("name", "age");
    }

    @Test
    void testResolvesBitmapIndexColumns() {
        CoreOptions options =
                new CoreOptions(
                        Collections.singletonMap("pk-bitmap.index.columns", " status,  region "));

        assertThat(options.primaryKeyBitmapIndexColumns()).containsExactly("status", "region");
    }

    @Test
    void testResolvesBTreeIndexOptions() {
        Map<String, String> values = new HashMap<>();
        values.put(
                "fields.name.pk-btree.index.options",
                "{\"block-size\":\"4 kb\",\"sorted-index.records-per-range\":\"10\"}");

        Options options = new CoreOptions(values).primaryKeyBTreeIndexOptions("name");

        assertThat(options.get("btree-index.block-size")).isEqualTo("4 kb");
        assertThat(options.get("sorted-index.records-per-range")).isEqualTo("10");
    }

    @Test
    void testResolvesBitmapIndexOptions() {
        Map<String, String> values = new HashMap<>();
        values.put(
                "fields.status.pk-bitmap.index.options",
                "{\"dictionary-block-size\":\"8 kb\","
                        + "\"sorted-index.records-per-range\":\"20\"}");

        Options options = new CoreOptions(values).primaryKeyBitmapIndexOptions("status");

        assertThat(options.get("bitmap-index.dictionary-block-size")).isEqualTo("8 kb");
        assertThat(options.get("sorted-index.records-per-range")).isEqualTo("20");
    }
}

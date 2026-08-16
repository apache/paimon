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

package org.apache.paimon.data.shredding;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapSharedShreddingContext}. */
class MapSharedShreddingContextTest {

    @Test
    void testFirstFileUsesKMax() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        assertThat(context.computeNextK()).containsEntry("tags", 256);
    }

    @Test
    void testAdaptKAfterOneFile() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        context.reportFileStats("tags", 7);

        assertThat(context.computeNextK()).containsEntry("tags", 7);
    }

    @Test
    void testAdaptKCappedByKMax() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 10));

        context.reportFileStats("tags", 20);

        assertThat(context.computeNextK()).containsEntry("tags", 10);
    }

    @Test
    void testWindowP90UsesMaxWhenSamplesAreClose() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        context.reportFileStats("tags", 3);
        context.reportFileStats("tags", 7);
        context.reportFileStats("tags", 5);

        assertThat(context.computeNextK()).containsEntry("tags", 7);
    }

    @Test
    void testWindowP90IgnoresSingleFarOutlier() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        for (int i = 0; i < 19; i++) {
            context.reportFileStats("tags", 3);
        }
        context.reportFileStats("tags", 1000);

        assertThat(context.computeNextK()).containsEntry("tags", 3);
    }

    @Test
    void testWindowP90UsesMaxWithinAbsoluteSlack() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        for (int i = 0; i < 19; i++) {
            context.reportFileStats("tags", 3);
        }
        context.reportFileStats("tags", 7);

        assertThat(context.computeNextK()).containsEntry("tags", 7);
    }

    @Test
    void testWindowP90UsesMaxWithinRelativeSlack() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        for (int i = 0; i < 19; i++) {
            context.reportFileStats("tags", 100);
        }
        context.reportFileStats("tags", 125);

        assertThat(context.computeNextK()).containsEntry("tags", 125);
    }

    @Test
    void testWindowP90IgnoresMaxBeyondBothSlacks() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        for (int i = 0; i < 19; i++) {
            context.reportFileStats("tags", 100);
        }
        context.reportFileStats("tags", 130);

        assertThat(context.computeNextK()).containsEntry("tags", 100);
    }

    @Test
    void testMultipleColumnsIndependent() {
        Map<String, Integer> columns = columns("tags", 256);
        columns.put("attrs", 128);
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns);

        context.reportFileStats("tags", 10);
        context.reportFileStats("attrs", 5);

        assertThat(context.computeNextK()).containsEntry("tags", 10).containsEntry("attrs", 5);
    }

    @Test
    void testGetShreddingColumnNamesSorted() {
        Map<String, Integer> columns = columns("tags", 256);
        columns.put("metrics", 64);
        columns.put("props", 128);
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns);

        assertThat(context.getShreddingColumnNames()).containsExactly("metrics", "props", "tags");
        assertThatThrownBy(() -> context.getShreddingColumnNames().add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSlidingWindowEvictsOldEntries() {
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns("tags", 256));

        context.reportFileStats("tags", 104);
        for (int i = 0; i < 19; i++) {
            context.reportFileStats("tags", 100);
        }
        assertThat(context.computeNextK()).containsEntry("tags", 104);

        context.reportFileStats("tags", 100);

        assertThat(context.computeNextK()).containsEntry("tags", 100);
    }

    private static Map<String, Integer> columns(String name, int maxColumns) {
        Map<String, Integer> columns = new LinkedHashMap<>();
        columns.put(name, maxColumns);
        return columns;
    }
}

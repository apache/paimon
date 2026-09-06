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

package org.apache.paimon.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.partition.PartitionStatistics;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link DelegateCatalog}. A missing forward does not fail: the interface default drops
 * down to the call that predates statistics, so only the report disappears.
 */
class DelegateCatalogTest {

    private static final Identifier IDENTIFIER = Identifier.create("db", "t");

    @Test
    void testCreatePartitionsCarriesStatisticsAndModeToTheWrappedCatalog() throws Exception {
        Catalog wrapped = mock(Catalog.class);
        Catalog delegating = new TestDelegateCatalog(wrapped);
        List<Map<String, String>> specs =
                Arrays.asList(
                        Collections.singletonMap("dt", "20260728"),
                        Collections.singletonMap("dt", "20260729"));
        List<PartitionStatistics> statistics =
                Collections.singletonList(
                        new PartitionStatistics(specs.get(0), 3L, 300L, 1L, 1000L, -1));

        delegating.createPartitions(IDENTIFIER, specs, true, statistics, false, null);

        verify(wrapped).createPartitions(IDENTIFIER, specs, true, statistics, false, null);
        // Falling through to the two-argument call is how the statistics would go missing.
        verify(wrapped, never()).createPartitions(any(), anyList());
    }

    @Test
    void testCreatePartitionsCarriesTheAbsenceOfStatisticsThrough() throws Exception {
        Catalog wrapped = mock(Catalog.class);
        Catalog delegating = new TestDelegateCatalog(wrapped);
        List<Map<String, String>> specs =
                Collections.singletonList(Collections.singletonMap("dt", "20260728"));

        delegating.createPartitions(IDENTIFIER, specs, false, null, false, null);

        verify(wrapped).createPartitions(IDENTIFIER, specs, false, null, false, null);
    }

    @Test
    void testCreatePartitionsCarriesOptionsToTheWrappedCatalog() throws Exception {
        Catalog wrapped = mock(Catalog.class);
        Catalog delegating = new TestDelegateCatalog(wrapped);
        Map<String, String> spec = Collections.singletonMap("dt", "20260728");
        List<Map<String, String>> specs = Collections.singletonList(spec);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), "file:/archive/dt=20260728");
        options.put("owner", "data-platform");
        List<Map<String, String>> partitionOptions = Collections.singletonList(options);

        delegating.createPartitions(IDENTIFIER, specs, true, null, false, partitionOptions);

        verify(wrapped).createPartitions(IDENTIFIER, specs, true, null, false, partitionOptions);
        verify(wrapped, never()).createPartitions(IDENTIFIER, specs, true, null, false, null);
    }

    /** {@link DelegateCatalog} forwards every operation; these tests never rebuild one. */
    private static class TestDelegateCatalog extends DelegateCatalog {

        TestDelegateCatalog(Catalog wrapped) {
            super(wrapped);
        }

        @Override
        public CatalogLoader catalogLoader() {
            return wrapped.catalogLoader();
        }
    }
}

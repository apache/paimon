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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.StatsTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for format-independent incremental file selection. */
public class DataEvolutionIncrementalCompactPlannerTest {

    @Test
    public void testDifferentRowRangesRequireWholeGroup() {
        List<DataFileMeta> files =
                Arrays.asList(
                        file("base", 0, 10, 1, 10000),
                        file("left", 0, 5, 2, 10),
                        file("right", 5, 5, 3, 10));
        assertThat(planner(false, true).selectFiles(files, 3)).containsExactlyElementsOf(files);
    }

    @Test
    public void testIncompleteColumnValuesKeepOlderProvidersLive() {
        DataFileMeta base = file("base", 0, 10, 1, 10000);
        DataFileMeta middle = file("middle", 0, 10, 2, 10);
        DataFileMeta latest = file("latest", 0, 10, 3, 10);
        List<DataFileMeta> files = Arrays.asList(base, middle, latest);
        // Complete newer columns supersede the whole base and trigger space amplification.
        assertThat(planner(true, true).selectFiles(files, 3)).containsExactlyElementsOf(files);
        // Row-level fallbacks can still need that base, even when all files write the same field.
        assertThat(planner(false, true).selectFiles(files, 3)).containsExactly(latest, middle);
    }

    @Test
    public void testUnsafePartialProjectionRequiresWholeGroup() {
        List<DataFileMeta> files =
                Arrays.asList(
                        file("base", 0, 10, 1, 10000),
                        file("middle", 0, 10, 2, 10),
                        file("latest", 0, 10, 3, 10));
        assertThat(planner(false, false).selectFiles(files, 3)).containsExactlyElementsOf(files);
    }

    private DataEvolutionIncrementalCompactPlanner planner(
            boolean completeColumnValues, boolean canRewrite) {
        return new DataEvolutionIncrementalCompactPlanner(
                new CoreOptions(new Options()),
                file -> Collections.singleton(0),
                file -> completeColumnValues,
                files -> canRewrite);
    }

    private DataFileMeta file(
            String name, long firstRowId, long rowCount, long sequence, long size) {
        return DataFileMeta.forAppend(
                name,
                size,
                rowCount,
                StatsTestUtils.newEmptySimpleStats(),
                sequence,
                sequence,
                0,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                Collections.singletonList("value"));
    }
}

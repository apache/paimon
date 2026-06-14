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

package org.apache.paimon.table.source;

import org.apache.paimon.format.FileSplitBoundary;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FineGrainedSplitGenerator}'s coalesce/cap algorithm. */
public class FineGrainedSplitGeneratorTest {

    @Test
    public void singleRowGroupReturnedAsIs() {
        List<FileSplitBoundary> in = Arrays.asList(new FileSplitBoundary(0, 1024, 10));
        List<FileSplitBoundary> out = FineGrainedSplitGenerator.coalesce(in, 4096, 100);
        assertThat(out).hasSize(1);
        assertThat(out.get(0).length()).isEqualTo(1024);
        assertThat(out.get(0).rowCount()).isEqualTo(10);
    }

    @Test
    public void coalescesUpToTargetSize() {
        List<FileSplitBoundary> in =
                Arrays.asList(
                        new FileSplitBoundary(0, 1000, 10),
                        new FileSplitBoundary(1000, 1000, 10),
                        new FileSplitBoundary(2000, 1000, 10),
                        new FileSplitBoundary(3000, 1000, 10));
        List<FileSplitBoundary> out = FineGrainedSplitGenerator.coalesce(in, 2000, 100);
        assertThat(out).hasSize(2);
        assertThat(out.get(0).length()).isEqualTo(2000);
        assertThat(out.get(0).rowCount()).isEqualTo(20);
        assertThat(out.get(1).offset()).isEqualTo(2000);
        assertThat(out.get(1).length()).isEqualTo(2000);
        assertThat(out.get(1).rowCount()).isEqualTo(20);
    }

    @Test
    public void singleOversizeRowGroupIsKept() {
        List<FileSplitBoundary> in = Arrays.asList(new FileSplitBoundary(0, 10000, 100));
        List<FileSplitBoundary> out = FineGrainedSplitGenerator.coalesce(in, 1000, 100);
        assertThat(out).hasSize(1);
        assertThat(out.get(0).length()).isEqualTo(10000);
    }

    @Test
    public void capLimitsCountAndAbsorbsTail() {
        List<FileSplitBoundary> in = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            in.add(new FileSplitBoundary(i * 1000L, 1000, 10));
        }
        List<FileSplitBoundary> out = FineGrainedSplitGenerator.coalesce(in, 500, 3);
        assertThat(out).hasSize(3);
        long totalLength = out.stream().mapToLong(FileSplitBoundary::length).sum();
        long totalRows = out.stream().mapToLong(FileSplitBoundary::rowCount).sum();
        assertThat(totalLength).isEqualTo(10_000L);
        assertThat(totalRows).isEqualTo(100L);
        assertThat(out.get(2).length()).isEqualTo(8000L);
        assertThat(out.get(2).rowCount()).isEqualTo(80L);
    }

    @Test
    public void capDoesNothingWhenUnderLimit() {
        List<FileSplitBoundary> in =
                Arrays.asList(
                        new FileSplitBoundary(0, 1000, 10), new FileSplitBoundary(1000, 1000, 10));
        List<FileSplitBoundary> out = FineGrainedSplitGenerator.coalesce(in, 500, 100);
        assertThat(out).hasSize(2);
    }
}

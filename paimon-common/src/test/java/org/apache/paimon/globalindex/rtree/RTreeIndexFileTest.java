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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.fs.Path;
import org.apache.paimon.geo.BoundingBox;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests source-row invariants before metadata is used to prune index files. */
class RTreeIndexFileTest {

    @ParameterizedTest
    @CsvSource({"2,50,50", "3,50,51", "1,50,51"})
    void testImpossibleSourceRowSpanIsRejectedBeforeRangePruning(
            int rows, long firstRow, long lastRow) {
        GlobalIndexIOMeta file = file(rows, firstRow, lastRow);
        RTreeGlobalIndexer indexer =
                new RTreeGlobalIndexer(
                        new DataField(0, "geom", DataTypes.GEOMETRY()), new Options());
        ExecutorService executor = newDirectExecutorService();
        try {
            // A valid CRC must not allow an impossible row range to bypass validation when the
            // requested range would otherwise prune the file without opening it.
            assertThatThrownBy(
                            () ->
                                    indexer.createReader(
                                            ignored -> {
                                                throw new AssertionError("Unexpected file read.");
                                            },
                                            Collections.singletonList(file),
                                            100,
                                            Collections.singletonList(new Range(0, 1)),
                                            executor))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasRootCauseMessage("Invalid R-tree header counts or row range.");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testSparseAndMaximumSourceRowSpansRemainValid() {
        for (long[] source :
                new long[][] {
                    {1, 0, 0},
                    {1, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1},
                    {2, 0, Long.MAX_VALUE - 1},
                    {3, Long.MAX_VALUE - 3, Long.MAX_VALUE - 1}
                }) {
            RTreeIndexFile.Metadata meta =
                    RTreeIndexFile.Metadata.read(
                            file((int) source[0], source[1], source[2]), Long.MAX_VALUE);
            assertThat(meta.rowCount()).isEqualTo((int) source[0]);
            assertThat(meta.firstRow).isEqualTo(source[1]);
            assertThat(meta.lastRow).isEqualTo(source[2]);
        }
    }

    private static GlobalIndexIOMeta file(int rows, long firstRow, long lastRow) {
        RTreeIndexFile.Metadata meta =
                new RTreeIndexFile.Metadata(2, 0, rows, 0, firstRow, lastRow, BoundingBox.empty());
        return new GlobalIndexIOMeta(new Path("rtree.index"), meta.fileSize(), rows, meta.bytes);
    }
}

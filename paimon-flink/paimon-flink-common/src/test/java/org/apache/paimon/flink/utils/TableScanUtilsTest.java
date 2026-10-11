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

package org.apache.paimon.flink.utils;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableScanUtils}. */
public class TableScanUtilsTest {

    @Test
    public void testSnapshotIdReadThroughTheAuthorizationWrapper() {
        FileStoreSourceSplit plain = sourceSplit(dataSplit(7L));
        FileStoreSourceSplit wrapped = sourceSplit(new QueryAuthSplit(plain.split(), authResult()));

        assertThat(TableScanUtils.getSnapshotId(plain)).hasValue(7L);
        assertThat(TableScanUtils.getSnapshotId(wrapped))
                .isEqualTo(TableScanUtils.getSnapshotId(plain));
    }

    @Test
    public void testSnapshotIdOfASplitWithoutOneIsEmpty() {
        Split noSnapshotId =
                new Split() {
                    @Override
                    public long rowCount() {
                        return 0;
                    }

                    @Override
                    public OptionalLong mergedRowCount() {
                        return OptionalLong.empty();
                    }
                };

        assertThat(TableScanUtils.getSnapshotId(sourceSplit(noSnapshotId)))
                .isEqualTo(Optional.empty());
        assertThat(
                        TableScanUtils.getSnapshotId(
                                sourceSplit(new QueryAuthSplit(noSnapshotId, authResult()))))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testSnapshotIdReadThroughTheFallbackAndAuthorizationWrappers() {
        FileStoreSourceSplit plain = sourceSplit(dataSplit(7L));
        FileStoreSourceSplit nested =
                sourceSplit(withFallback(new QueryAuthSplit(plain.split(), authResult())));

        assertThat(nested.split()).isNotInstanceOf(DataSplit.class);
        assertThat(TableScanUtils.getSnapshotId(nested))
                .isEqualTo(TableScanUtils.getSnapshotId(plain))
                .hasValue(7L);
    }

    @Test
    public void testSnapshotIdOfAFallbackDataSplitIsTheSnapshotItCopies() {
        FileStoreSourceSplit fallback = sourceSplit(withFallback(dataSplit(7L)));

        assertThat(fallback.split()).isInstanceOf(DataSplit.class);
        assertThat(TableScanUtils.getSnapshotId(fallback)).hasValue(7L);
    }

    private static TableQueryAuthResult authResult() {
        return new TableQueryAuthResult(null, null);
    }

    private static Split withFallback(Split split) {
        return FallbackReadFileStoreTable.toFallbackSplit(split, true);
    }

    private static DataSplit dataSplit(long snapshotId) {
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withDataFiles(Collections.emptyList())
                .isStreaming(true)
                .withBucketPath("/temp/xxx")
                .build();
    }

    private static FileStoreSourceSplit sourceSplit(Split split) {
        return new FileStoreSourceSplit(UUID.randomUUID().toString(), split, 0);
    }
}

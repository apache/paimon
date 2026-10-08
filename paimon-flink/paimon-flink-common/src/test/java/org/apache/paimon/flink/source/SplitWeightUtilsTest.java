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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.QueryAuthSplit;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for split assignment weights of metadata-only imports. */
class SplitWeightUtilsTest {

    @Test
    void testUnknownIncrementalCountHasPositiveFileSizeFallback() {
        IncrementalSplit split = mock(IncrementalSplit.class);
        when(split.rowCount()).thenReturn(DataFileMeta.UNKNOWN_ROW_COUNT);
        QueryAuthSplit wrapped = mock(QueryAuthSplit.class);
        when(wrapped.split()).thenReturn(split);
        Options options = new Options();
        options.set(
                FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_WEIGHT_MODE,
                FlinkConnectorOptions.SplitWeightMode.FILE_SIZE);
        assertThat(
                        SplitWeightUtils.splitWeightFunc(options)
                                .apply(new FileStoreSourceSplit("unknown", split)))
                .isEqualTo(1L);
        assertThat(
                        SplitWeightUtils.splitWeightFunc(options)
                                .apply(new FileStoreSourceSplit("wrapped", wrapped)))
                .isEqualTo(1L);
    }

    @Test
    void testUnknownRowCountUsesFileSizeForAssignment() {
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.fileSize()).thenReturn(123L);
        DataSplit split = mock(DataSplit.class);
        when(split.rowCount()).thenReturn(DataFileMeta.UNKNOWN_ROW_COUNT);
        when(split.dataFiles()).thenReturn(Collections.singletonList(file));

        assertThat(
                        SplitWeightUtils.splitWeightFunc(new Options())
                                .apply(new FileStoreSourceSplit("unknown", split)))
                .isEqualTo(123L);
        when(split.rowCount()).thenReturn(3L);
        assertThat(
                        SplitWeightUtils.splitWeightFunc(new Options())
                                .apply(new FileStoreSourceSplit("known", split)))
                .isEqualTo(3L);
    }
}

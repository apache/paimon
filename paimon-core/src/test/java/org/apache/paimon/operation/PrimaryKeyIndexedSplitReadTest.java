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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PrimaryKeyVectorPositionReader;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PrimaryKeyIndexedSplitRead}. */
class PrimaryKeyIndexedSplitReadTest {

    @Test
    void testConvertsRangesToPhysicalPositionsAndDelegates() throws Exception {
        DataSplit dataSplit = dataSplit();
        IndexedSplit split =
                new IndexedSplit(
                        dataSplit,
                        Arrays.asList(new Range(1, 1), new Range(3, 3)),
                        new float[] {0.5F, 0.25F});
        RawFileSplitRead rawRead = mock(RawFileSplitRead.class);
        FileRecordReader<InternalRow> fileReader = mock(FileRecordReader.class);
        when(rawRead.createFileReader(eq(dataSplit), any(RoaringBitmap32.class)))
                .thenReturn(fileReader);

        RecordReader<InternalRow> reader =
                new PrimaryKeyIndexedSplitRead(rawRead).createReader(split);

        assertThat(reader).isInstanceOf(PrimaryKeyVectorPositionReader.class);
        ArgumentCaptor<RoaringBitmap32> positions = ArgumentCaptor.forClass(RoaringBitmap32.class);
        verify(rawRead).createFileReader(eq(dataSplit), positions.capture());
        assertThat(positions.getValue()).isEqualTo(RoaringBitmap32.bitmapOf(1, 3));
    }

    private static DataSplit dataSplit() {
        DataFileMeta dataFile =
                DataFileMeta.forAppend(
                        "data-file",
                        100,
                        5,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null);
        return DataSplit.builder()
                .withSnapshot(3)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withTotalBuckets(1)
                .withDataFiles(Collections.singletonList(dataFile))
                .build();
    }
}

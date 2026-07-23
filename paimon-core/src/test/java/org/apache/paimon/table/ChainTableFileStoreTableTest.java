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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplitImpl;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for split routing in {@link ChainTableFileStoreTable}. */
public class ChainTableFileStoreTableTest {

    @Test
    public void testQueryAuthChainSplitRoutedToChainGroupRead() throws Exception {
        assertQueryAuthSplitRoutedToChainGroupRead(mock(ChainSplit.class));
    }

    @Test
    public void testQueryAuthDataSplitRoutedToChainGroupRead() throws Exception {
        assertQueryAuthSplitRoutedToChainGroupRead(mock(DataSplit.class));
    }

    @Test
    public void testFallbackSplitStillUsesFallbackRead() throws Exception {
        ReadFixture fixture = new ReadFixture();
        QueryAuthSplit wrappedSplit = queryAuthSplit(mock(DataSplit.class));
        FallbackSplitImpl fallbackSplit = new FallbackSplitImpl(wrappedSplit, false);
        RecordReader<InternalRow> reader = mock(RecordReader.class);
        when(fixture.wrappedRead.createReader(wrappedSplit)).thenReturn(reader);

        assertThat(fixture.table.newRead().createReader(fallbackSplit)).isSameAs(reader);

        verify(fixture.wrappedRead).createReader(wrappedSplit);
    }

    private void assertQueryAuthSplitRoutedToChainGroupRead(Split wrappedSplit) throws IOException {
        ReadFixture fixture = new ReadFixture();
        QueryAuthSplit split = queryAuthSplit(wrappedSplit);
        RecordReader<InternalRow> reader = mock(RecordReader.class);
        when(fixture.chainGroupRead.createReader(split)).thenReturn(reader);

        assertThat(fixture.table.newRead().createReader(split)).isSameAs(reader);

        verify(fixture.chainGroupRead).createReader(split);
        verify(fixture.wrappedRead, never()).createReader(any(Split.class));
    }

    private static QueryAuthSplit queryAuthSplit(Split split) {
        return new QueryAuthSplit(
                split, new TableQueryAuthResult(null, Collections.singletonMap("v", "mask")));
    }

    private static class ReadFixture {

        private final FileStoreTable wrapped = mock(FileStoreTable.class);
        private final ChainGroupReadTable chainGroup = mock(ChainGroupReadTable.class);
        private final InnerTableRead wrappedRead = mock(InnerTableRead.class);
        private final InnerTableRead chainGroupRead = mock(InnerTableRead.class);
        private final ChainTableFileStoreTable table;

        private ReadFixture() {
            when(wrapped.coreOptions()).thenReturn(CoreOptions.fromMap(Collections.emptyMap()));
            when(wrapped.newRead()).thenReturn(wrappedRead);
            when(chainGroup.newRead()).thenReturn(chainGroupRead);
            table = new ChainTableFileStoreTable(wrapped, chainGroup);
        }
    }
}

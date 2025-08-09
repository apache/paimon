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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionSplitReadProvider}. */
public class DataEvolutionSplitReadProviderTest {

    private Supplier<DataEvolutionSplitRead> mockSupplier;
    private SplitReadConfig mockSplitReadConfig;
    private DataEvolutionSplitRead mockSplitRead;
    private DataEvolutionSplitReadProvider provider;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        mockSupplier = (Supplier<DataEvolutionSplitRead>) mock(Supplier.class);
        mockSplitReadConfig = mock(SplitReadConfig.class);
        mockSplitRead = mock(DataEvolutionSplitRead.class);
        when(mockSupplier.get()).thenReturn(mockSplitRead);

        provider = new DataEvolutionSplitReadProvider(mockSupplier, mockSplitReadConfig);
    }

    @Test
    public void testGetAndInitialization() {
        // Supplier should not be called yet due to lazy initialization
        verify(mockSupplier, times(0)).get();

        // First access, should trigger initialization
        DataEvolutionSplitRead read = provider.get().get();

        // Verify supplier and config were called
        verify(mockSupplier, times(1)).get();
        verify(mockSplitReadConfig, times(1)).config(mockSplitRead);
        assertThat(read).isSameAs(mockSplitRead);

        // Second access, should return cached instance without re-initializing
        DataEvolutionSplitRead read2 = provider.get().get();
        verify(mockSupplier, times(1)).get();
        verify(mockSplitReadConfig, times(1)).config(mockSplitRead);
        assertThat(read2).isSameAs(mockSplitRead);
    }

    @Test
    public void testMatchWithNoFiles() {
        DataSplit split = mock(DataSplit.class);
        when(split.dataFiles()).thenReturn(Collections.emptyList());
        assertThat(provider.match(split, false)).isFalse();
    }

    @Test
    public void testMatchWithOneFile() {
        DataSplit split = mock(DataSplit.class);
        DataFileMeta file1 = mock(DataFileMeta.class);
        when(split.dataFiles()).thenReturn(Collections.singletonList(file1));
        assertThat(provider.match(split, false)).isFalse();
    }

    @Test
    public void testMatchWithNullFirstRowId() {
        DataSplit split = mock(DataSplit.class);
        DataFileMeta file1 = mock(DataFileMeta.class);
        DataFileMeta file2 = mock(DataFileMeta.class);

        when(file1.firstRowId()).thenReturn(1L);
        when(file2.firstRowId()).thenReturn(null);
        when(split.dataFiles()).thenReturn(Arrays.asList(file1, file2));

        assertThat(provider.match(split, false)).isFalse();
    }

    @Test
    public void testMatchWithDifferentFirstRowIds() {
        DataSplit split = mock(DataSplit.class);
        DataFileMeta file1 = mock(DataFileMeta.class);
        DataFileMeta file2 = mock(DataFileMeta.class);

        when(file1.firstRowId()).thenReturn(1L);
        when(file2.firstRowId()).thenReturn(2L);
        when(split.dataFiles()).thenReturn(Arrays.asList(file1, file2));

        assertThat(provider.match(split, false)).isFalse();
    }

    @Test
    public void testMatchSuccess() {
        DataSplit split = mock(DataSplit.class);
        DataFileMeta file1 = mock(DataFileMeta.class);
        DataFileMeta file2 = mock(DataFileMeta.class);

        when(file1.firstRowId()).thenReturn(100L);
        when(file2.firstRowId()).thenReturn(100L);
        when(split.dataFiles()).thenReturn(Arrays.asList(file1, file2));

        // The forceKeepDelete parameter is not used in match, so test both values
        assertThat(provider.match(split, true)).isTrue();
        assertThat(provider.match(split, false)).isTrue();
    }
}

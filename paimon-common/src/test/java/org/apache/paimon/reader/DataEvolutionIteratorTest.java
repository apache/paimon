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

package org.apache.paimon.reader;

import org.apache.paimon.data.InternalRow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionIterator}. */
public class DataEvolutionIteratorTest {

    private DataEvolutionRow mockRow;
    private RecordReader.RecordIterator<InternalRow> iterator1;
    private RecordReader.RecordIterator<InternalRow> iterator2;
    private InternalRow row1;
    private InternalRow row2;

    @BeforeEach
    public void setUp() {
        mockRow = mock(DataEvolutionRow.class);
        iterator1 = mock(RecordReader.RecordIterator.class);
        iterator2 = mock(RecordReader.RecordIterator.class);
        row1 = mock(InternalRow.class);
        row2 = mock(InternalRow.class);
    }

    @Test
    public void testNextWithData() throws IOException {
        when(iterator1.next()).thenReturn(row1).thenReturn(null);
        when(iterator2.next()).thenReturn(row2).thenReturn(null);

        DataEvolutionIterator evolutionIterator =
                new DataEvolutionIterator(
                        mockRow, new RecordReader.RecordIterator[] {iterator1, iterator2});

        // First call to next()
        InternalRow result = evolutionIterator.next();
        assertThat(result).isSameAs(mockRow);

        InOrder inOrder = inOrder(iterator1, iterator2, mockRow);
        inOrder.verify(iterator1).next();
        inOrder.verify(mockRow).setRow(0, row1);
        inOrder.verify(iterator2).next();
        inOrder.verify(mockRow).setRow(1, row2);

        // Second call to next() should return null
        InternalRow nullResult = evolutionIterator.next();
        assertThat(nullResult).isNull();
        verify(iterator1, times(2)).next();
        verify(iterator2, times(1)).next(); // Should not be called again
    }

    @Test
    public void testNextWhenFirstIteratorIsEmpty() throws IOException {
        when(iterator1.next()).thenReturn(null);

        DataEvolutionIterator evolutionIterator =
                new DataEvolutionIterator(
                        mockRow, new RecordReader.RecordIterator[] {iterator1, iterator2});

        InternalRow result = evolutionIterator.next();
        assertThat(result).isNull();

        verify(iterator1).next();
        verify(iterator2, never()).next();
        verify(mockRow, never()).setRow(anyInt(), any());
    }

    @Test
    public void testNextWithNullIteratorInArray() throws IOException {
        when(iterator1.next()).thenReturn(row1).thenReturn(null);
        when(iterator2.next()).thenReturn(row2).thenReturn(null);

        DataEvolutionIterator evolutionIterator =
                new DataEvolutionIterator(
                        mockRow, new RecordReader.RecordIterator[] {iterator1, null, iterator2});

        InternalRow result = evolutionIterator.next();
        assertThat(result).isSameAs(mockRow);

        InOrder inOrder = inOrder(iterator1, iterator2, mockRow);
        inOrder.verify(iterator1).next();
        inOrder.verify(mockRow).setRow(0, row1);
        inOrder.verify(iterator2).next();
        inOrder.verify(mockRow).setRow(2, row2);
        verify(mockRow, never()).setRow(1, null); // Check that index 1 is skipped

        // Next call returns null
        InternalRow nullResult = evolutionIterator.next();
        assertThat(nullResult).isNull();
    }

    @Test
    public void testNextWithEmptyIterators() throws IOException {
        DataEvolutionIterator evolutionIterator =
                new DataEvolutionIterator(mockRow, new RecordReader.RecordIterator[0]);

        InternalRow result = evolutionIterator.next();
        assertThat(result).isSameAs(mockRow);

        verify(mockRow, never()).setRow(anyInt(), any());
    }

    @Test
    public void testReleaseBatch() {
        RecordReader.RecordIterator<InternalRow> iterator3 =
                mock(RecordReader.RecordIterator.class);
        DataEvolutionIterator evolutionIterator =
                new DataEvolutionIterator(
                        mockRow,
                        new RecordReader.RecordIterator[] {iterator1, null, iterator2, iterator3});

        evolutionIterator.releaseBatch();

        verify(iterator1).releaseBatch();
        verify(iterator2).releaseBatch();
        verify(iterator3).releaseBatch();
    }
}

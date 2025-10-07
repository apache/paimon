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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DataPagedOutputSerializer}. */
class DataPagedOutputSerializerTest {

    @Test
    void testSmallDatasetStaysInInitialPhase() throws IOException {
        InternalRowSerializer serializer = createSimpleSerializer();
        DataPagedOutputSerializer output = new DataPagedOutputSerializer(serializer, 256, 8 * 1024);
        SimpleCollectingOutputView pagedView =
                new SimpleCollectingOutputView(
                        new ArrayList<>(),
                        () -> MemorySegment.allocateHeapMemory(4 * 1024),
                        4 * 1024);

        BinaryRow row = createSampleRow();

        // Write a few small rows that fit within the initial buffer
        for (int i = 0; i < 50; i++) {
            output.write(row);
            serializer.serializeToPages(row, pagedView);
        }

        // Should still be using initial buffer since we haven't exceeded page size
        assertThat(output.pagedOut()).isNull();

        // assert result
        SimpleCollectingOutputView result = output.close();
        assertEqual(result, pagedView);
    }

    @Test
    void testLargeDatasetSwitchesToPagedMode() throws IOException {
        InternalRowSerializer serializer = createSimpleSerializer();
        DataPagedOutputSerializer output = new DataPagedOutputSerializer(serializer, 256, 4 * 1024);
        SimpleCollectingOutputView pagedView =
                new SimpleCollectingOutputView(
                        new ArrayList<>(),
                        () -> MemorySegment.allocateHeapMemory(4 * 1024),
                        4 * 1024);

        BinaryRow row = createSampleRow();

        // Write many rows
        for (int i = 0; i < 500; i++) {
            output.write(row);
            serializer.serializeToPages(row, pagedView);
        }

        assertThat(output.pagedOut()).isNotNull();

        // assert result
        SimpleCollectingOutputView result = output.close();
        assertEqual(result, pagedView);
    }

    private void assertEqual(SimpleCollectingOutputView view1, SimpleCollectingOutputView view2) {
        assertThat(view1.getCurrentOffset()).isEqualTo(view2.getCurrentOffset());
        assertThat(view1.fullSegments().size()).isEqualTo(view2.fullSegments().size());
        for (int i = 0; i < view1.fullSegments().size(); i++) {
            MemorySegment segment1 = view1.fullSegments().get(i);
            MemorySegment segment2 = view2.fullSegments().get(i);
            assertThat(segment1.size()).isEqualTo(segment2.size());
            assertThat(segment1.getHeapMemory()).isEqualTo(segment2.getHeapMemory());
        }
    }

    private InternalRowSerializer createSimpleSerializer() {
        return new InternalRowSerializer(DataTypes.INT(), DataTypes.STRING());
    }

    private BinaryRow createSampleRow() {
        GenericRow genericRow =
                GenericRow.of(
                        42, BinaryString.fromString("sample-data-" + System.currentTimeMillis()));
        InternalRowSerializer tempSerializer = createSimpleSerializer();
        return tempSerializer.toBinaryRow(genericRow);
    }
}

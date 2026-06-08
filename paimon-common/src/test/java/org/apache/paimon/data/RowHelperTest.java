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

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowHelper}, focusing on the resetIfTooLarge(BinaryRow) behavior. */
class RowHelperTest {

    @Test
    void testReleasesWhenTransitionFromLargeToSmallRecord() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.BYTES()));

        // Write a large record (> 4MB) to inflate the internal buffer
        byte[] largePayload = new byte[5 * 1024 * 1024];
        GenericRow largeRow = GenericRow.of(BinaryString.fromString("key"), largePayload);
        largeRow.setRowKind(RowKind.INSERT);
        helper.copyInto(largeRow);
        BinaryRow reuseAfterLarge = helper.reuseRow();
        assertThat(reuseAfterLarge).isNotNull();

        // Hysteresis: should NOT release when current record is large
        helper.resetIfTooLarge(reuseAfterLarge);
        assertThat(helper.reuseRow()).isNotNull();

        // Write a small record — buffer is still oversized from the large record
        GenericRow smallRow = GenericRow.of(BinaryString.fromString("s"), new byte[10]);
        smallRow.setRowKind(RowKind.INSERT);
        helper.copyInto(smallRow);
        BinaryRow reuseAfterSmall = helper.reuseRow();

        // Should release now: buffer > 4MB but current record < 4MB
        helper.resetIfTooLarge(reuseAfterSmall);
        assertThat(helper.reuseRow()).isNull();
    }

    @Test
    void testKeepsSmallBuffer() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.INT()));

        GenericRow smallRow = GenericRow.of(BinaryString.fromString("hello"), 42);
        smallRow.setRowKind(RowKind.INSERT);
        helper.copyInto(smallRow);
        BinaryRow reuse = helper.reuseRow();
        assertThat(reuse).isNotNull();

        // Small buffer should NOT be released
        helper.resetIfTooLarge(reuse);
        assertThat(helper.reuseRow()).isNotNull();
    }

    @Test
    void testSkipsWhenCurrentRowIsNotReuseRow() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.BYTES()));

        // Write a large record to inflate the buffer
        byte[] largePayload = new byte[5 * 1024 * 1024];
        GenericRow largeRow = GenericRow.of(BinaryString.fromString("key"), largePayload);
        largeRow.setRowKind(RowKind.INSERT);
        helper.copyInto(largeRow);
        assertThat(helper.reuseRow()).isNotNull();

        // Simulate the BinaryRow input path: toBinaryRow() returns the input directly,
        // not the helper's reuseRow. Pass a different BinaryRow instance.
        BinaryRow externalRow = new BinaryRow(2);
        externalRow.pointTo(MemorySegment.wrap(new byte[32]), 0, 32);

        // Should NOT release because externalRow != reuseRow
        helper.resetIfTooLarge(externalRow);
        assertThat(helper.reuseRow()).isNotNull();
    }

    @Test
    void testSafeToCallWithNullReuseRow() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING()));
        assertThat(helper.reuseRow()).isNull();

        // Should be safe — reuseRow is null, no NPE
        BinaryRow someRow = new BinaryRow(1);
        someRow.pointTo(MemorySegment.wrap(new byte[16]), 0, 16);
        helper.resetIfTooLarge(someRow);
        assertThat(helper.reuseRow()).isNull();
    }

    @Test
    void testReuseRecreatedAfterRelease() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.BYTES()));

        // Inflate buffer, then transition to small
        byte[] largePayload = new byte[5 * 1024 * 1024];
        GenericRow largeRow = GenericRow.of(BinaryString.fromString("key"), largePayload);
        largeRow.setRowKind(RowKind.INSERT);
        helper.copyInto(largeRow);

        GenericRow smallRow = GenericRow.of(BinaryString.fromString("small"), new byte[10]);
        smallRow.setRowKind(RowKind.INSERT);
        helper.copyInto(smallRow);

        helper.resetIfTooLarge(helper.reuseRow());
        assertThat(helper.reuseRow()).isNull();

        // Write another small record — reuseRow should be recreated
        helper.copyInto(smallRow);
        assertThat(helper.reuseRow()).isNotNull();

        // Small buffer should survive
        helper.resetIfTooLarge(helper.reuseRow());
        assertThat(helper.reuseRow()).isNotNull();
    }
}

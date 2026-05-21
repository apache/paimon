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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowHelper}, focusing on the resetIfTooLarge() behavior. */
class RowHelperTest {

    @Test
    void testResetIfTooLargeReleasesOversizedBuffer() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.BYTES()));

        // Write a large record (> 4MB) to inflate the internal buffer
        byte[] largePayload = new byte[5 * 1024 * 1024]; // 5MB
        Arrays.fill(largePayload, (byte) 'x');
        GenericRow largeRow = GenericRow.of(BinaryString.fromString("key"), largePayload);
        largeRow.setRowKind(RowKind.INSERT);
        helper.copyInto(largeRow);

        assertThat(helper.reuseRow()).isNotNull();

        // resetIfTooLarge() should release the bloated buffer
        helper.resetIfTooLarge();
        assertThat(helper.reuseRow()).isNull();
    }

    @Test
    void testResetIfTooLargeKeepsSmallBuffer() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.INT()));

        // Write a small record (< 4MB)
        GenericRow smallRow = GenericRow.of(BinaryString.fromString("hello"), 42);
        smallRow.setRowKind(RowKind.INSERT);
        helper.copyInto(smallRow);

        assertThat(helper.reuseRow()).isNotNull();

        // resetIfTooLarge() should NOT release the small buffer
        helper.resetIfTooLarge();
        assertThat(helper.reuseRow()).isNotNull();
    }

    @Test
    void testResetIfTooLargeBeforeCopyInto() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING()));

        // reuseRow is null before any copyInto
        assertThat(helper.reuseRow()).isNull();

        // resetIfTooLarge() should be safe to call when reuseRow is null
        helper.resetIfTooLarge();
        assertThat(helper.reuseRow()).isNull();
    }

    @Test
    void testReuseIsRecreatedAfterRelease() {
        RowHelper helper = new RowHelper(Arrays.asList(DataTypes.STRING(), DataTypes.BYTES()));

        // Write a large record to inflate the buffer
        byte[] largePayload = new byte[5 * 1024 * 1024];
        GenericRow largeRow = GenericRow.of(BinaryString.fromString("key"), largePayload);
        largeRow.setRowKind(RowKind.INSERT);
        helper.copyInto(largeRow);
        helper.resetIfTooLarge();
        assertThat(helper.reuseRow()).isNull();

        // Write a small record — reuseRow should be recreated
        GenericRow smallRow = GenericRow.of(BinaryString.fromString("small"), new byte[10]);
        smallRow.setRowKind(RowKind.INSERT);
        helper.copyInto(smallRow);
        assertThat(helper.reuseRow()).isNotNull();

        // Small buffer should survive resetIfTooLarge()
        helper.resetIfTooLarge();
        assertThat(helper.reuseRow()).isNotNull();
    }
}

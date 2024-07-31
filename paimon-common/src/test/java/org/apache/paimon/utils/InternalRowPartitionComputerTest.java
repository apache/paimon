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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InternalRowPartitionComputer}. */
public class InternalRowPartitionComputerTest {

    @Test
    public void testPartitionToString() {
        RowType rowType = RowType.of();
        BinaryRow binaryRow = new BinaryRow(0);
        assertThat(InternalRowPartitionComputer.toSimpleString(rowType, binaryRow, "-", 30))
                .isEqualTo("");

        rowType = RowType.of(DataTypes.STRING(), DataTypes.INT());
        binaryRow = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeString(0, BinaryString.fromString("20240731"));
        writer.writeInt(1, 10);
        assertThat(InternalRowPartitionComputer.toSimpleString(rowType, binaryRow, "-", 30))
                .isEqualTo("20240731-10");

        rowType = RowType.of(DataTypes.STRING(), DataTypes.INT());
        binaryRow = new BinaryRow(2);
        writer = new BinaryRowWriter(binaryRow);
        writer.setNullAt(0);
        writer.writeInt(1, 10);
        assertThat(InternalRowPartitionComputer.toSimpleString(rowType, binaryRow, "-", 30))
                .isEqualTo("null-10");
    }
}

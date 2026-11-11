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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Test for {@link InternalRowPartitionComputer}. */
public class InternalRowPartitionComputerTest {

    @Test
    public void testPartitionToString() {
        RowType rowType = RowType.of();
        BinaryRow binaryRow = new BinaryRow(0);
        assertThat(InternalRowPartitionComputer.partToSimpleString(rowType, binaryRow, "-", 30))
                .isEqualTo("");

        rowType = RowType.of(DataTypes.STRING(), DataTypes.INT());
        binaryRow = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeString(0, BinaryString.fromString("20240731"));
        writer.writeInt(1, 10);
        assertThat(InternalRowPartitionComputer.partToSimpleString(rowType, binaryRow, "-", 30))
                .isEqualTo("20240731-10");

        rowType = RowType.of(DataTypes.STRING(), DataTypes.INT());
        binaryRow = new BinaryRow(2);
        writer = new BinaryRowWriter(binaryRow);
        writer.setNullAt(0);
        writer.writeInt(1, 10);
        assertThat(InternalRowPartitionComputer.partToSimpleString(rowType, binaryRow, "-", 30))
                .isEqualTo("null-10");
    }

    @Test
    public void testPartitionColumnNotAtRowPrefix() {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "dt", "region", "extra"});
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        "__DEFAULT_PARTITION__", rowType, new String[] {"region", "dt"}, false);

        BinaryRow binaryRow = new BinaryRow(4);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, 1);
        writer.writeString(1, BinaryString.fromString("20240731"));
        writer.writeString(2, BinaryString.fromString("hangzhou"));
        writer.writeString(3, BinaryString.fromString("ignored"));
        writer.complete();

        // "region" sits at row position 2, which is out of range for an array sized by the
        // number of partition columns.
        assertThat(computer.generatePartValues(binaryRow))
                .containsExactly(entry("region", "hangzhou"), entry("dt", "20240731"));
    }

    @Test
    public void testPartitionColumnsReorderedWithinRowPrefix() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"id", "dt", "region"});
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        "__DEFAULT_PARTITION__", rowType, new String[] {"dt", "id"}, false);

        BinaryRow binaryRow = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, 1);
        writer.writeString(1, BinaryString.fromString("20240731"));
        writer.writeString(2, BinaryString.fromString("hangzhou"));
        writer.complete();

        // Both row positions are in range here, so indexing by row position did not throw: it
        // paired each column name with the other column's getter and cast executor.
        assertThat(computer.generatePartValues(binaryRow))
                .containsExactly(entry("dt", "20240731"), entry("id", "1"));
    }

    @Test
    public void testBinaryPartitionNamesUseStableUtf8Values() {
        DataType[] binaryTypes = {DataTypes.BINARY(16), DataTypes.VARBINARY(16), DataTypes.BYTES()};
        byte[][] values = {
            null,
            new byte[0],
            " \t\u001c".getBytes(StandardCharsets.UTF_8),
            "a/b=c".getBytes(StandardCharsets.UTF_8),
            "汉字".getBytes(StandardCharsets.UTF_8),
            "\u00a0".getBytes(StandardCharsets.UTF_8),
            new byte[] {(byte) 0xed, (byte) 0xa0, (byte) 0x80}
        };
        String[] expected = {
            "__DEFAULT_PARTITION__",
            "__DEFAULT_PARTITION__",
            "__DEFAULT_PARTITION__",
            "a/b=c",
            "汉字",
            "\u00a0",
            "\ufffd"
        };
        for (boolean legacy : new boolean[] {true, false}) {
            for (DataType type : binaryTypes) {
                RowType rowType =
                        RowType.of(
                                new DataType[] {DataTypes.INT(), type}, new String[] {"id", "p"});
                InternalRowPartitionComputer computer =
                        new InternalRowPartitionComputer(
                                "__DEFAULT_PARTITION__", rowType, new String[] {"p"}, legacy);
                for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
                    BinaryRow row = new BinaryRow(2);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    writer.writeInt(0, 123);
                    byte[] value = values[valueIndex];
                    if (value == null) {
                        writer.setNullAt(1);
                    } else {
                        writer.writeBinary(1, value, 0, value.length);
                    }
                    writer.complete();
                    // BinaryRow.getBinary creates a fresh byte[] on every call. Partition
                    // names must depend on its contents, including after row serialization.
                    assertThat(computer.generatePartValues(row))
                            .containsExactly(entry("p", expected[valueIndex]));
                    assertThat(computer.generatePartValues(row))
                            .containsExactly(entry("p", expected[valueIndex]));
                    assertThat(computer.generatePartValues(row.copy()))
                            .containsExactly(entry("p", expected[valueIndex]));
                }
            }
        }
    }

    @Test
    public void testBinaryPartitionKeepsOtherLegacyValues() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.DATE(), DataTypes.BYTES()},
                        new String[] {"dt", "p"});
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        "__DEFAULT_PARTITION__", rowType, new String[] {"p", "dt"}, true);
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, 1);
        writer.writeBinary(1, new byte[] {65}, 0, 1);
        writer.complete();
        assertThat(computer.generatePartValues(row))
                .containsExactly(entry("p", "A"), entry("dt", "1"));
    }
}

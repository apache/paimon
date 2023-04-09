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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link RowDataPartitionComputer}. */
public class RowDataPartitionComputerTest {

    @Test
    public void testGeneratePartitionValues() {
        RowDataPartitionComputer partitionComputer =
                FileStorePathFactory.getPartitionComputer(
                        RowType.of(
                                new DataType[] {new DateType(), new IntType(), new TimeType()},
                                new String[] {"dt", "hour", "time"}),
                        FileStorePathFactory.PARTITION_DEFAULT_NAME.defaultValue());

        LocalDate localDate = LocalDate.now();
        int hour = LocalTime.now().getHour();
        int time = LocalTime.now().toSecondOfDay() * 1000;
        BinaryRow binaryRow = buildPartitionBinaryRow(localDate, hour, time);
        LinkedHashMap<String, String> map = partitionComputer.generatePartValues(binaryRow);

        assertEquals(map.get("dt"), localDate.toString());
        assertEquals(map.get("hour"), String.valueOf(hour));
        assertEquals(map.get("time"), partitionComputer.formatTimeTypePart(time));
    }

    public BinaryRow buildPartitionBinaryRow(LocalDate dt, Integer hour, Integer time) {
        BinaryRow partition = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        if (dt != null) {
            writer.writeInt(0, (int) dt.toEpochDay());
        } else {
            writer.setNullAt(0);
        }
        if (hour != null) {
            writer.writeInt(1, hour);
        } else {
            writer.setNullAt(1);
        }
        if (time != null) {
            writer.writeInt(2, time);
        } else {
            writer.setNullAt(2);
        }
        writer.complete();
        return partition;
    }
}

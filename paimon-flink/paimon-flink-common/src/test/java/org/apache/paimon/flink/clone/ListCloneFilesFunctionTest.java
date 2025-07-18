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

package org.apache.paimon.flink.clone;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.clone.files.ListCloneFilesFunction;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.flink.clone.files.ListCloneFilesFunction.getPartitionPredicate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ListCloneFilesFunction}. */
public class ListCloneFilesFunctionTest {

    @Test
    public void testConvertSqlToPartitionPredicate() throws Exception {
        Identifier tableId = Identifier.create("test_db", "test_table");
        RowType partitionType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .build();

        PartitionPredicate p = getPartitionPredicate("a=1", partitionType, tableId);
        assertThat(p.test(twoColumnsPartition(1, "2"))).isTrue();
        assertThat(p.test(twoColumnsPartition(2, "1"))).isFalse();

        p = getPartitionPredicate("a=1 OR b='2'", partitionType, tableId);
        assertThat(p.test(twoColumnsPartition(1, "1"))).isTrue();
        assertThat(p.test(twoColumnsPartition(2, "2"))).isTrue();
        assertThat(p.test(twoColumnsPartition(2, "1"))).isFalse();

        // c not in partition fields
        assertThatThrownBy(() -> getPartitionPredicate("a=1 OR c=1", partitionType, tableId))
                .hasMessage(
                        "Failed to parse partition filter sql 'a=1 OR c=1' for table test_db.test_table");

        // no partition keys
        assertThatThrownBy(() -> getPartitionPredicate("a=1", RowType.of(), tableId))
                .hasMessage(
                        "Failed to parse partition filter sql 'a=1' for table test_db.test_table");
    }

    private static BinaryRow twoColumnsPartition(int a, String b) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeInt(0, a);
        writer.writeString(1, BinaryString.fromString(b));
        writer.complete();
        return row;
    }
}

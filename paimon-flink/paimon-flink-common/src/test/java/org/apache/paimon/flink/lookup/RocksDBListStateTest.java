/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Test for {@link org.apache.paimon.flink.lookup.RocksDBListState}. */
public class RocksDBListStateTest {

    @TempDir Path tempDir;

    @Test
    void test() throws Exception {
        RocksDBStateFactory factory = new RocksDBStateFactory(tempDir.toString(), new Options());

        RowType keyType = RowType.of(DataTypes.STRING());
        RowType valueType = RowType.of(DataTypes.STRING());
        RocksDBListState listState =
                factory.listState(
                        "test",
                        new InternalRowSerializer(keyType),
                        new InternalRowSerializer(valueType),
                        1);

        GenericRow key = row("aaa");
        listState.add(key, row("1"));
        List<InternalRow> result = listState.get(key);
        Assertions.assertEquals(Lists.newArrayList("1"), getString(result));
        listState.add(key, row("2,3"));
        Assertions.assertEquals(Lists.newArrayList("1", "2,3"), getString(listState.get(key)));
        listState.add(key, row("1"));
        Assertions.assertEquals(Lists.newArrayList("1", "2,3", "1"), getString(listState.get(key)));
        Assertions.assertEquals(Lists.newArrayList("1", "2,3", "1"), getString(listState.get(key)));
        Assertions.assertTrue(listState.get(row("bbb")).isEmpty());
    }

    public GenericRow row(String value) {
        return GenericRow.of(bs(value));
    }

    public GenericRow row(String value, RowKind kind) {
        GenericRow row = GenericRow.of(bs(value));
        row.setRowKind(kind);
        return row;
    }

    public BinaryString bs(String v) {
        return BinaryString.fromString(v);
    }

    public BinaryRow write(String v) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter write = new BinaryRowWriter(row);
        write.writeString(0, bs(v));
        return row;
    }

    public List<String> getString(List<InternalRow> inputs) {
        List<String> rows = new ArrayList<>();
        for (InternalRow input : inputs) {
            rows.add(input.getString(0).toString());
        }
        return rows;
    }
}

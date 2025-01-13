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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generate test data for {@link FlinkCdcSyncTableSinkITCase} and {@link
 * FlinkCdcSyncDatabaseSinkITCase}.
 */
public class TestTable {

    private final RowType initialRowType;

    private final Queue<TestCdcEvent> events;
    private final Map<Integer, Map<String, String>> expected;

    public TestTable(
            String tableName,
            int numEvents,
            int numSchemaChanges,
            int numPartitions,
            int numKeys,
            boolean isAppendTable) {
        List<String> fieldNames = new ArrayList<>();
        List<Boolean> isBigInt = new ArrayList<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numFields = random.nextInt(5) + 1;

        DataType[] initialFieldTypes = new DataType[2 + numFields];
        initialFieldTypes[0] = DataTypes.INT();
        initialFieldTypes[1] = DataTypes.INT();

        String[] initialFieldNames = new String[2 + numFields];
        initialFieldNames[0] = "pt";
        initialFieldNames[1] = "k";

        for (int i = 0; i < numFields; i++) {
            fieldNames.add("v" + i);
            initialFieldNames[2 + i] = "v" + i;

            if (random.nextBoolean()) {
                isBigInt.add(true);
                initialFieldTypes[2 + i] = DataTypes.BIGINT();
            } else {
                isBigInt.add(false);
                initialFieldTypes[2 + i] = DataTypes.INT();
            }
        }

        initialRowType = RowType.of(initialFieldTypes, initialFieldNames);

        Set<Integer> schemaChangePositions = new HashSet<>();

        // if numSchemaChanges is larger than numEvents, the following loop will never end
        // we can, of course, use fisher's algorithm or such, but let's make things simple for tests
        numSchemaChanges = Math.min(numSchemaChanges, numEvents / 2);
        for (int i = 0; i < numSchemaChanges; i++) {
            int pos;
            do {
                pos = random.nextInt(numEvents);
            } while (schemaChangePositions.contains(pos));
            schemaChangePositions.add(pos);
        }

        events = new LinkedList<>();
        expected = new HashMap<>();
        for (int i = 0; i < numEvents; i++) {
            if (schemaChangePositions.contains(i)) {
                if (random.nextBoolean()) {
                    int idx = random.nextInt(fieldNames.size());
                    isBigInt.set(idx, true);
                } else {
                    String newName = "v" + fieldNames.size();
                    fieldNames.add(newName);
                    isBigInt.add(false);
                }
                events.add(new TestCdcEvent(tableName, currentDataFieldList(fieldNames, isBigInt)));
            } else {
                Map<String, String> data = new HashMap<>();
                int key = random.nextInt(numKeys);
                data.put("k", String.valueOf(key));
                int pt = key % numPartitions;
                data.put("pt", String.valueOf(pt));

                for (int j = 0; j < fieldNames.size(); j++) {
                    String fieldName = fieldNames.get(j);
                    if (isBigInt.get(j)) {
                        data.put(fieldName, String.valueOf(random.nextLong()));
                    } else {
                        data.put(fieldName, String.valueOf(random.nextInt()));
                    }
                }

                List<CdcRecord> records = new ArrayList<>();
                // Generate test data for pk table
                if (!isAppendTable) {
                    boolean shouldInsert = true;
                    if (expected.containsKey(key)) {
                        records.add(new CdcRecord(RowKind.DELETE, expected.get(key)));
                        expected.remove(key);
                        // 20% chance to only delete without insert
                        shouldInsert = random.nextInt(5) > 0;
                    }
                    if (shouldInsert) {
                        records.add(new CdcRecord(RowKind.INSERT, data));
                        expected.put(key, data);
                    }
                }
                // Generate test data for append table
                else {
                    if (expected.containsKey(key)) {
                        records.add(new CdcRecord(RowKind.DELETE, expected.get(key)));
                    } else {
                        records.add(new CdcRecord(RowKind.INSERT, data));
                        expected.put(key, data);
                    }
                }
                events.add(new TestCdcEvent(tableName, records, Objects.hash(tableName, key)));
            }
        }
    }

    private List<DataField> currentDataFieldList(List<String> fieldNames, List<Boolean> isBigInt) {
        List<DataField> fields = new ArrayList<>();

        // pt
        fields.add(initialRowType.getFields().get(0));
        // k
        fields.add(initialRowType.getFields().get(1));

        for (int i = 0; i < fieldNames.size(); i++) {
            fields.add(
                    new DataField(
                            2 + i,
                            fieldNames.get(i),
                            isBigInt.get(i) ? DataTypes.BIGINT() : DataTypes.INT()));
        }

        return fields;
    }

    public RowType initialRowType() {
        return initialRowType;
    }

    public Queue<TestCdcEvent> events() {
        return events;
    }

    public void assertResult(TableSchema schema, Iterator<InternalRow> it) {
        Map<Integer, Map<String, String>> actual = new HashMap<>();
        while (it.hasNext()) {
            InternalRow row = it.next();
            Map<String, String> fields = new HashMap<>();
            for (int i = 0; i < schema.fieldNames().size(); i++) {
                if (!row.isNullAt(i)) {
                    fields.put(
                            schema.fieldNames().get(i),
                            String.valueOf(
                                    schema.fields().get(i).type().equals(DataTypes.BIGINT())
                                            ? row.getLong(i)
                                            : row.getInt(i)));
                }
            }
            actual.put(Integer.valueOf(fields.get("k")), fields);
        }
        assertThat(actual).isEqualTo(expected);
    }
}

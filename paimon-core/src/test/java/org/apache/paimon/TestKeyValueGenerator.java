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

package org.apache.paimon;

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SchemaEvolutionTableTestBase;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.paimon.TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED;
import static org.apache.paimon.TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED;
import static org.apache.paimon.TestKeyValueGenerator.GeneratorMode.SINGLE_PARTITIONED;

/** Random {@link KeyValue} generator. */
public class TestKeyValueGenerator {

    // record structure
    //
    // * dt: string, 20201110 ~ 20201111 -------|
    // * hr: int, 9 ~ 10                 -------+----> partition
    //
    // * shopId:  int, 0 ~ 9             -------|
    // * orderId: long, any value        -------+----> primary key
    //
    // * itemId: long, any value
    // * price & amount: int[], [1 ~ 100, 1 ~ 10]
    // * comment: string, length from 10 to 1000
    public static final RowType DEFAULT_ROW_TYPE =
            RowType.of(
                    new DataField(0, "dt", new VarCharType(false, 8)),
                    new DataField(1, "hr", new IntType(false)),
                    new DataField(2, "shopId", new IntType(false)),
                    new DataField(3, "orderId", new BigIntType(false)),
                    new DataField(4, "itemId", new BigIntType()),
                    new DataField(5, "priceAmount", new ArrayType(new IntType())),
                    new DataField(6, "comment", new VarCharType(Integer.MAX_VALUE)));
    public static final RowType DEFAULT_PART_TYPE =
            new RowType(DEFAULT_ROW_TYPE.getFields().subList(0, 2));

    public static final RowType SINGLE_PARTITIONED_ROW_TYPE =
            DEFAULT_ROW_TYPE.project("dt", "shopId", "orderId", "itemId", "priceAmount", "comment");

    public static final RowType SINGLE_PARTITIONED_PART_TYPE =
            RowType.of(SINGLE_PARTITIONED_ROW_TYPE.getTypeAt(0));

    public static final RowType NON_PARTITIONED_ROW_TYPE =
            DEFAULT_ROW_TYPE.project("shopId", "orderId", "itemId", "priceAmount", "comment");
    public static final RowType NON_PARTITIONED_PART_TYPE = RowType.of();

    public static final List<String> KEY_NAME_LIST = Arrays.asList("shopId", "orderId");

    public static final RowType KEY_TYPE =
            RowType.of(
                    new DataField(
                            2 + SpecialFields.KEY_FIELD_ID_START,
                            SpecialFields.KEY_FIELD_PREFIX + "shopId",
                            new IntType(false)),
                    new DataField(
                            3 + SpecialFields.KEY_FIELD_ID_START,
                            SpecialFields.KEY_FIELD_PREFIX + "orderId",
                            new BigIntType(false)));

    public static final InternalRowSerializer DEFAULT_ROW_SERIALIZER =
            new InternalRowSerializer(DEFAULT_ROW_TYPE);
    public static final InternalRowSerializer KEY_SERIALIZER = new InternalRowSerializer(KEY_TYPE);
    public static final RecordComparator KEY_COMPARATOR =
            (a, b) -> {
                int firstResult = a.getInt(0) - b.getInt(0);
                if (firstResult != 0) {
                    return firstResult;
                }
                return Long.compare(a.getLong(1), b.getLong(1));
            };

    private final GeneratorMode mode;
    private final Random random;

    private final List<Order> addedOrders;
    private final List<Order> deletedOrders;

    private long sequenceNumber;

    private final InternalRowSerializer rowSerializer;
    private final InternalRowSerializer partitionSerializer;

    public TestKeyValueGenerator() {
        this(MULTI_PARTITIONED);
    }

    public TestKeyValueGenerator(GeneratorMode mode) {
        this.mode = mode;
        this.random = new Random();

        this.addedOrders = new ArrayList<>();
        this.deletedOrders = new ArrayList<>();

        this.sequenceNumber = 0;

        RowType rowType;
        RowType partitionType;
        switch (mode) {
            case NON_PARTITIONED:
                rowType = NON_PARTITIONED_ROW_TYPE;
                partitionType = NON_PARTITIONED_PART_TYPE;
                break;
            case SINGLE_PARTITIONED:
                rowType = SINGLE_PARTITIONED_ROW_TYPE;
                partitionType = SINGLE_PARTITIONED_PART_TYPE;
                break;
            case MULTI_PARTITIONED:
                rowType = DEFAULT_ROW_TYPE;
                partitionType = DEFAULT_PART_TYPE;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported generator mode: " + mode);
        }
        rowSerializer = new InternalRowSerializer(rowType);
        partitionSerializer = new InternalRowSerializer(partitionType);
    }

    public KeyValue next() {
        int op = random.nextInt(5);
        Order order = null;
        RowKind kind = RowKind.INSERT;
        if (op == 0 && addedOrders.size() > 0) {
            // delete order
            order = pick(addedOrders);
            deletedOrders.add(order);
            kind = RowKind.DELETE;
        } else if (op == 1) {
            // update order
            if (random.nextBoolean() && deletedOrders.size() > 0) {
                order = pick(deletedOrders);
            } else if (addedOrders.size() > 0) {
                order = pick(addedOrders);
            }
            if (order != null) {
                order.update();
                addedOrders.add(order);
                kind = RowKind.INSERT;
            }
        }
        if (order == null) {
            // new order
            order = new Order();
            addedOrders.add(order);
            kind = RowKind.INSERT;
        }
        return new KeyValue()
                .replace(
                        KEY_SERIALIZER
                                .toBinaryRow(GenericRow.of(order.shopId, order.orderId))
                                .copy(),
                        sequenceNumber++,
                        kind,
                        rowSerializer.toBinaryRow(convertToRow(order)).copy());
    }

    public KeyValue nextInsert(
            String dt,
            int hr,
            @Nullable Long itemId,
            @Nullable int[] priceMount,
            @Nullable String comment) {
        Order order = new Order();
        order.dt = dt;
        order.hr = hr;
        order.itemId = itemId;
        order.priceAmount = priceMount;
        order.comment = comment;
        return new KeyValue()
                .replace(
                        KEY_SERIALIZER
                                .toBinaryRow(GenericRow.of(order.shopId, order.orderId))
                                .copy(),
                        sequenceNumber++,
                        RowKind.INSERT,
                        rowSerializer.toBinaryRow(convertToRow(order)).copy());
    }

    // used for FileStoreExpireDeleteDirTest to generate data in specified partition
    public KeyValue nextPartitionedData(RowKind kind, Object... partitionSpec) {
        Order order = new Order();
        switch (mode) {
            case MULTI_PARTITIONED:
                assert partitionSpec.length == 2;
                order.dt = (String) partitionSpec[0];
                order.hr = (int) partitionSpec[1];
                break;
            case SINGLE_PARTITIONED:
                assert partitionSpec.length == 1;
                order.dt = (String) partitionSpec[0];
                break;
            default:
                // do nothing
        }
        return new KeyValue()
                .replace(
                        KEY_SERIALIZER
                                .toBinaryRow(GenericRow.of(order.shopId, order.orderId))
                                .copy(),
                        sequenceNumber++,
                        kind,
                        rowSerializer.toBinaryRow(convertToRow(order)).copy());
    }

    private InternalRow convertToRow(Order order) {
        List<Object> values =
                new ArrayList<>(
                        Arrays.asList(
                                order.shopId,
                                order.orderId,
                                order.itemId,
                                order.priceAmount == null
                                        ? null
                                        : new GenericArray(order.priceAmount),
                                BinaryString.fromString(order.comment)));

        if (mode == MULTI_PARTITIONED) {
            values.add(0, BinaryString.fromString(order.dt));
            values.add(1, order.hr);
        } else if (mode == SINGLE_PARTITIONED) {
            values.add(0, BinaryString.fromString(order.dt));
        }
        return GenericRow.of(values.toArray(new Object[0]));
    }

    public BinaryRow getPartition(KeyValue kv) {
        Object[] values;
        if (mode == MULTI_PARTITIONED) {
            values = new Object[] {kv.value().getString(0), kv.value().getInt(1)};
        } else if (mode == SINGLE_PARTITIONED) {
            values = new Object[] {kv.value().getString(0)};
        } else {
            values = new Object[0];
        }
        return partitionSerializer.toBinaryRow(GenericRow.of(values)).copy();
    }

    public static List<String> getPrimaryKeys(GeneratorMode mode) {
        List<String> trimmedPk =
                KEY_TYPE.getFieldNames().stream()
                        .map(f -> f.replaceFirst(SpecialFields.KEY_FIELD_PREFIX, ""))
                        .collect(Collectors.toList());
        if (mode != NON_PARTITIONED) {
            trimmedPk = new ArrayList<>(trimmedPk);
            trimmedPk.addAll(
                    mode == MULTI_PARTITIONED
                            ? DEFAULT_PART_TYPE.getFieldNames()
                            : SINGLE_PARTITIONED_PART_TYPE.getFieldNames());
        }
        return trimmedPk;
    }

    public static Map<String, String> toPartitionMap(BinaryRow partition, GeneratorMode mode) {
        Map<String, String> map = new HashMap<>();
        if (mode == MULTI_PARTITIONED) {
            map.put("dt", partition.getString(0).toString());
            map.put("hr", String.valueOf(partition.getInt(1)));
        } else if (mode == SINGLE_PARTITIONED) {
            map.put("dt", partition.getString(0).toString());
        }
        return map;
    }

    public static SchemaManager createTestSchemaManager(Path path) {
        TableSchema tableSchema =
                new TableSchema(
                        0,
                        TableSchema.newFields(DEFAULT_ROW_TYPE),
                        DEFAULT_ROW_TYPE.getFieldCount(),
                        Collections.EMPTY_LIST,
                        KEY_NAME_LIST,
                        Collections.EMPTY_MAP,
                        "");
        Map<Long, TableSchema> schemas = new HashMap<>();
        schemas.put(tableSchema.id(), tableSchema);
        return new SchemaEvolutionTableTestBase.TestingSchemaManager(path, schemas);
    }

    public void sort(List<KeyValue> kvs) {
        kvs.sort(
                (a, b) -> {
                    int keyCompareResult = KEY_COMPARATOR.compare(a.key(), b.key());
                    return keyCompareResult != 0
                            ? keyCompareResult
                            : Long.compare(a.sequenceNumber(), b.sequenceNumber());
                });
    }

    private Order pick(List<Order> list) {
        int idx = random.nextInt(list.size());
        Order tmp = list.get(idx);
        list.set(idx, list.get(list.size() - 1));
        list.remove(list.size() - 1);
        return tmp;
    }

    private class Order {
        private String dt;
        private int hr;
        private final int shopId;
        private final long orderId;
        @Nullable private Long itemId;
        @Nullable private int[] priceAmount;
        @Nullable private String comment;

        private Order() {
            dt = String.valueOf(random.nextInt(2) + 20211110);
            hr = random.nextInt(2) + 8;
            shopId = random.nextInt(10);
            orderId = random.nextLong();
            update();
        }

        private String randomString(int length) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < length; i++) {
                builder.append((char) (random.nextInt(127 - 32) + 32));
            }
            return builder.toString();
        }

        private void update() {
            itemId = random.nextInt(10) == 0 ? null : random.nextLong();
            priceAmount =
                    random.nextInt(10) == 0
                            ? null
                            : new int[] {random.nextInt(100) + 1, random.nextInt(100) + 1};
            comment = random.nextInt(10) == 0 ? null : randomString(random.nextInt(1001 - 10) + 10);
        }
    }

    /** Generator mode for {@link TestKeyValueGenerator}. */
    public enum GeneratorMode {
        NON_PARTITIONED,
        SINGLE_PARTITIONED,
        MULTI_PARTITIONED
    }

    /** {@link KeyValueFieldsExtractor} implementation for test. */
    public static class TestKeyValueFieldsExtractor implements KeyValueFieldsExtractor {
        private static final long serialVersionUID = 1L;

        public static final TestKeyValueFieldsExtractor EXTRACTOR =
                new TestKeyValueFieldsExtractor();

        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return schema.fields().stream()
                    .filter(f -> KEY_NAME_LIST.contains(f.name()))
                    .map(
                            f ->
                                    new DataField(
                                            f.id() + SpecialFields.KEY_FIELD_ID_START,
                                            SpecialFields.KEY_FIELD_PREFIX + f.name(),
                                            f.type(),
                                            f.description()))
                    .collect(Collectors.toList());
        }

        @Override
        public List<DataField> valueFields(TableSchema schema) {
            return schema.fields();
        }
    }
}

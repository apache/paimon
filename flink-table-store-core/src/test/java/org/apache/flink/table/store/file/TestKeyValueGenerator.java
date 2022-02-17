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

package org.apache.flink.table.store.file;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
    public static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new VarCharType(false, 8),
                        new IntType(false),
                        new IntType(false),
                        new BigIntType(false),
                        new BigIntType(),
                        new ArrayType(new IntType()),
                        new VarCharType(Integer.MAX_VALUE)
                    },
                    new String[] {
                        "dt", "hr", "shopId", "orderId", "itemId", "priceAmount", "comment"
                    });
    public static final RowType PARTITION_TYPE =
            RowType.of(
                    new LogicalType[] {new VarCharType(false, 8), new IntType(false)},
                    new String[] {"dt", "hr"});
    public static final RowType KEY_TYPE =
            RowType.of(
                    new LogicalType[] {new IntType(false), new BigIntType(false)},
                    new String[] {"key_shopId", "key_orderId"});

    public static final RowDataSerializer ROW_SERIALIZER = new RowDataSerializer(ROW_TYPE);
    private static final RowDataSerializer PARTITION_SERIALIZER =
            new RowDataSerializer(PARTITION_TYPE);
    public static final RowDataSerializer KEY_SERIALIZER = new RowDataSerializer(KEY_TYPE);
    public static final RecordComparator KEY_COMPARATOR =
            (a, b) -> {
                int firstResult = a.getInt(0) - b.getInt(0);
                if (firstResult != 0) {
                    return firstResult;
                }
                return Long.compare(a.getLong(1), b.getLong(1));
            };

    private final Random random;

    private final List<Order> addedOrders;
    private final List<Order> deletedOrders;

    private long sequenceNumber;

    public TestKeyValueGenerator() {
        this.random = new Random();

        this.addedOrders = new ArrayList<>();
        this.deletedOrders = new ArrayList<>();

        this.sequenceNumber = 0;
    }

    public KeyValue next() {
        int op = random.nextInt(5);
        Order order = null;
        ValueKind kind = ValueKind.ADD;
        if (op == 0 && addedOrders.size() > 0) {
            // delete order
            order = pick(addedOrders);
            deletedOrders.add(order);
            kind = ValueKind.DELETE;
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
                kind = ValueKind.ADD;
            }
        }
        if (order == null) {
            // new order
            order = new Order();
            addedOrders.add(order);
            kind = ValueKind.ADD;
        }
        return new KeyValue()
                .replace(
                        KEY_SERIALIZER
                                .toBinaryRow(GenericRowData.of(order.shopId, order.orderId))
                                .copy(),
                        sequenceNumber++,
                        kind,
                        ROW_SERIALIZER
                                .toBinaryRow(
                                        GenericRowData.of(
                                                StringData.fromString(order.dt),
                                                order.hr,
                                                order.shopId,
                                                order.orderId,
                                                order.itemId,
                                                order.priceAmount == null
                                                        ? null
                                                        : new GenericArrayData(order.priceAmount),
                                                StringData.fromString(order.comment)))
                                .copy());
    }

    public BinaryRowData getPartition(KeyValue kv) {
        return PARTITION_SERIALIZER
                .toBinaryRow(GenericRowData.of(kv.value().getString(0), kv.value().getInt(1)))
                .copy();
    }

    public static Map<String, String> toPartitionMap(BinaryRowData partition) {
        Map<String, String> map = new HashMap<>();
        map.put("dt", partition.getString(0).toString());
        map.put("hr", String.valueOf(partition.getInt(1)));
        return map;
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
        private final String dt;
        private final int hr;
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
}

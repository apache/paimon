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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.OffsetRow;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.types.DataTypeChecks.getNestedTypes;
import static org.apache.paimon.types.DataTypeFamily.CONSTRUCTED;
import static org.apache.paimon.types.DataTypeRoot.BLOB;
import static org.apache.paimon.types.DataTypeRoot.VECTOR;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Projects and serializes payload-free {@link DeleteOrderingGuard}s for one table schema.
 *
 * <p>Source ordering fields are stored in schema order, retaining their original field ids. Sort
 * direction and deletion rules belong to the table schema, identified by {@link #schemaId()}.
 * Readers must use that schema, rather than the latest schema.
 *
 * <p>SST keys append a segment-local ordinal to the primary key. This preserves multiple retracts
 * for the same key without relying on equal-key SST seeks or prematurely discarding history. The
 * ordinal is not an event ordering watermark.
 */
public class DeleteOrderingGuardSerializer {

    private final long schemaId;
    private final RowType keyType;
    private final RowType orderingType;
    private final boolean ignoreDelete;
    private final ProjectedRow orderingProjection;
    private final ProjectedRow keyProjection;
    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer orderingSerializer;
    private final InternalRowSerializer storageKeySerializer;
    private final InternalRowSerializer storageValueSerializer;
    private final RowType storageKeyType;

    public DeleteOrderingGuardSerializer(
            long schemaId, RowType keyType, RowType valueType, CoreOptions options) {
        checkArgument(keyType.getFieldCount() > 0, "Delete ordering guards require primary keys.");
        checkArgument(
                options.mergeEngine() == DEDUPLICATE,
                "Delete ordering guards support only the deduplicate merge engine, but found '%s'.",
                options.mergeEngine());
        checkArgument(
                !options.definedAggFunc(),
                "Delete ordering guards do not support configured field aggregate functions.");
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.ignoreDelete = options.ignoreDelete();

        Set<String> orderingFields = orderingFields(options);
        for (String field : orderingFields) {
            checkArgument(
                    valueType.getFieldIndex(field) >= 0,
                    "Ordering field '%s' can not be found in table schema.",
                    field);
            checkArgument(
                    !CoreOptions.blobField(options.toMap()).contains(field),
                    "Delete ordering guards cannot retain BLOB ordering field '%s'.",
                    field);
        }
        List<DataField> fields = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            DataField field = valueType.getFields().get(i);
            if (orderingFields.contains(field.name())) {
                checkMetadataType(field.type());
                fields.add(field);
                indexes.add(i);
            }
        }
        keyType.getFieldTypes().forEach(DeleteOrderingGuardSerializer::checkMetadataType);
        this.orderingType = new RowType(fields);
        this.orderingProjection =
                ProjectedRow.from(indexes.stream().mapToInt(Integer::intValue).toArray());
        this.keyProjection =
                ProjectedRow.from(IntStream.range(0, keyType.getFieldCount()).toArray());
        this.keySerializer = new InternalRowSerializer(keyType);
        this.orderingSerializer = new InternalRowSerializer(orderingType);

        List<DataType> storageKeyTypes = new ArrayList<>(keyType.getFieldTypes());
        storageKeyTypes.add(DataTypes.BIGINT().notNull());
        this.storageKeyType = RowType.of(storageKeyTypes.toArray(new DataType[0]));
        this.storageKeySerializer = new InternalRowSerializer(storageKeyType);
        List<DataType> storageValueTypes = new ArrayList<>();
        storageValueTypes.add(DataTypes.BIGINT().notNull());
        storageValueTypes.add(DataTypes.TINYINT().notNull());
        storageValueTypes.add(DataTypes.INT().notNull());
        storageValueTypes.addAll(orderingType.getFieldTypes());
        this.storageValueSerializer =
                new InternalRowSerializer(RowType.of(storageValueTypes.toArray(new DataType[0])));
    }

    public static Set<String> orderingFields(CoreOptions options) {
        return new HashSet<>(options.sequenceField());
    }

    public long schemaId() {
        return schemaId;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType orderingType() {
        return orderingType;
    }

    /** Copies only key and ordering fields; no payload getter or serializer is invoked. */
    @Nullable
    public DeleteOrderingGuard extract(KeyValue record) {
        if (record.isAdd() || ignoreDelete) {
            return null;
        }
        return new DeleteOrderingGuard(
                keySerializer.copy(record.key()),
                orderingSerializer.copy(orderingProjection.replaceRow(record.value())),
                record.sequenceNumber(),
                record.valueKind(),
                record.level());
    }

    byte[] serializeKey(DeleteOrderingGuard guard, long ordinal) {
        checkArgument(
                guard.key().getFieldCount() == keyType.getFieldCount(),
                "Guard key does not match its schema.");
        // SST retains its last key until the block is flushed; the row serializer reuses its
        // buffer.
        return storageKeySerializer
                .toBinaryRow(new JoinedRow(guard.key(), GenericRow.of(ordinal)))
                .copy()
                .toBytes();
    }

    byte[] serializeValue(DeleteOrderingGuard guard) {
        checkArgument(
                guard.ordering().getFieldCount() == orderingType.getFieldCount(),
                "Guard ordering fields do not match their schema.");
        return storageValueSerializer
                .toBinaryRow(
                        new JoinedRow(
                                GenericRow.of(
                                        guard.sequenceNumber(),
                                        guard.valueKind().toByteValue(),
                                        guard.level()),
                                guard.ordering()))
                .copy()
                .toBytes();
    }

    DeleteOrderingGuard deserialize(MemorySlice key, MemorySlice value) {
        BinaryRow storageKey = new BinaryRow(storageKeyType.getFieldCount());
        storageKey.pointTo(key.segment(), key.offset(), key.length());
        BinaryRow storageValue = new BinaryRow(orderingType.getFieldCount() + 3);
        storageValue.pointTo(value.segment(), value.offset(), value.length());
        return new DeleteOrderingGuard(
                keySerializer.copy(keyProjection.replaceRow(storageKey)),
                orderingSerializer.copy(
                        new OffsetRow(orderingType.getFieldCount(), 3).replace(storageValue)),
                storageValue.getLong(0),
                RowKind.fromByteValue(storageValue.getByte(1)),
                storageValue.getInt(2));
    }

    Comparator<MemorySlice> keyComparator() {
        Comparator<InternalRow> comparator = new KeyComparatorSupplier(storageKeyType).get();
        BinaryRow left = new BinaryRow(storageKeyType.getFieldCount());
        BinaryRow right = new BinaryRow(storageKeyType.getFieldCount());
        return (a, b) -> {
            left.pointTo(a.segment(), a.offset(), a.length());
            right.pointTo(b.segment(), b.offset(), b.length());
            return comparator.compare(left, right);
        };
    }

    private static void checkMetadataType(DataType type) {
        checkArgument(
                !type.isAnyOf(BLOB, VECTOR),
                "Delete ordering guards cannot retain BLOB or VECTOR metadata.");
        if (type.is(CONSTRUCTED)) {
            getNestedTypes(type).forEach(DeleteOrderingGuardSerializer::checkMetadataType);
        }
    }
}

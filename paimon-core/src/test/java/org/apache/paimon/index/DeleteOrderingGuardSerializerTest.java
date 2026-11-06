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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests payload-free projection and eligibility of delete ordering guards. */
class DeleteOrderingGuardSerializerTest {

    private static final RowType KEY_TYPE = RowType.of(DataTypes.INT());

    @Test
    void testCapturesSourceSequencesAndDetachesReusedInput() {
        RowType valueType =
                RowType.builder()
                        .field("first", DataTypes.INT())
                        .field("payload", DataTypes.STRING())
                        .field("second", DataTypes.BIGINT())
                        .build();
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "second,first");
        DeleteOrderingGuardSerializer serializer = serializer(valueType, options);
        GenericRow key = GenericRow.of(1);
        GenericRow value = GenericRow.of(4, null, 7L);
        DeleteOrderingGuard guard =
                serializer.extract(
                        new KeyValue().replace(key, 19, RowKind.DELETE, value).setLevel(2));

        key.setField(0, 99);
        value.setField(0, 100);
        value.setField(2, 200L);
        assertThat(serializer.orderingType().getFields())
                .containsExactly(valueType.getField("first"), valueType.getField("second"));
        assertThat(guard.key().getInt(0)).isEqualTo(1);
        assertThat(guard.ordering().getInt(0)).isEqualTo(4);
        assertThat(guard.ordering().getLong(1)).isEqualTo(7L);
        assertThat(guard.sequenceNumber()).isEqualTo(19);
        assertThat(guard.valueKind()).isEqualTo(RowKind.DELETE);
        assertThat(guard.level()).isEqualTo(2);
    }

    @Test
    void testCompositeSequenceCopiesOnlyOrderingFields() {
        RowType valueType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("blob", DataTypes.BLOB())
                        .field("bMajor", DataTypes.INT())
                        .field("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                        .field("bMinor", DataTypes.BIGINT())
                        .field("payload", DataTypes.STRING())
                        .build();
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "bMajor,bMinor,a");
        options.set(CoreOptions.SEQUENCE_FIELD_SORT_ORDER, CoreOptions.SortOrder.DESCENDING);
        DeleteOrderingGuardSerializer serializer = serializer(valueType, options);
        InternalRow value = mock(InternalRow.class);
        when(value.getRowKind()).thenReturn(RowKind.INSERT);
        when(value.getInt(0)).thenReturn(4);
        when(value.isNullAt(2)).thenReturn(true);
        when(value.getLong(4)).thenReturn(7L);
        DeleteOrderingGuard guard =
                serializer.extract(
                        new KeyValue().replace(GenericRow.of(1), 31, RowKind.UPDATE_BEFORE, value));

        assertThat(serializer.orderingType().getFieldNames())
                .containsExactly("a", "bMajor", "bMinor");
        assertThat(guard.ordering().getInt(0)).isEqualTo(4);
        assertThat(guard.ordering().isNullAt(1)).isTrue();
        assertThat(guard.ordering().getLong(2)).isEqualTo(7L);
        assertThat(guard.valueKind()).isEqualTo(RowKind.UPDATE_BEFORE);
        verify(value, never()).getBlob(1);
        verify(value, never()).getVector(3);
        verify(value, never()).getString(5);
    }

    @Test
    void testCompositeSequenceIsDetached() {
        RowType sequenceType = RowType.of(DataTypes.INT(), DataTypes.BIGINT());
        RowType valueType = RowType.builder().field("seq", sequenceType).build();
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "seq");
        GenericRow sequence = GenericRow.of(4, 7L);
        DeleteOrderingGuard guard =
                serializer(valueType, options)
                        .extract(
                                new KeyValue()
                                        .replace(
                                                GenericRow.of(1),
                                                1,
                                                RowKind.DELETE,
                                                GenericRow.of(sequence)));
        sequence.setField(0, 99);
        assertThat(guard.ordering().getRow(0, 2).getInt(0)).isEqualTo(4);
        assertThat(guard.ordering().getRow(0, 2).getLong(1)).isEqualTo(7L);
    }

    @Test
    void testNoSourceSequenceReadsNoPayload() {
        InternalRow value = mock(InternalRow.class);
        when(value.getRowKind()).thenReturn(RowKind.INSERT);
        DeleteOrderingGuard guard =
                serializer(RowType.of(DataTypes.BLOB()), new Options())
                        .extract(
                                new KeyValue().replace(GenericRow.of(1), 4, RowKind.DELETE, value));
        assertThat(guard.ordering().getFieldCount()).isZero();
        assertThat(guard.sequenceNumber()).isEqualTo(4);
        verify(value, never()).getBlob(0);
    }

    @ParameterizedTest
    @EnumSource(RowKind.class)
    void testOnlyEffectiveRetractsProduceGuards(RowKind kind) {
        RowType valueType = RowType.of(DataTypes.INT());
        KeyValue record = new KeyValue().replace(GenericRow.of(1), 4, kind, GenericRow.of(9));
        assertThat(serializer(valueType, new Options()).extract(record) != null)
                .isEqualTo(kind.isRetract());
        Options options = new Options();
        options.set(CoreOptions.IGNORE_DELETE, true);
        assertThat(serializer(valueType, options).extract(record)).isNull();
    }

    @ParameterizedTest
    @EnumSource(
            value = CoreOptions.MergeEngine.class,
            names = {"DEDUPLICATE"},
            mode = EnumSource.Mode.EXCLUDE)
    void testUnsupportedMergeEnginesRejected(CoreOptions.MergeEngine engine) {
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, engine);
        assertThatThrownBy(() -> serializer(RowType.of(DataTypes.INT()), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only the deduplicate merge engine");
    }

    @ParameterizedTest
    @ValueSource(strings = {"fields.f0.aggregate-function", "fields.default-aggregate-function"})
    void testConfiguredFieldAggregationRejected(String key) {
        Options options = new Options();
        options.set(key, "sum");
        assertThatThrownBy(() -> serializer(RowType.of(DataTypes.INT()), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("configured field aggregate functions");
    }

    @Test
    void testMissingOrderingFieldRejected() {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "missing");
        assertThatThrownBy(() -> serializer(RowType.of(DataTypes.INT()), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Ordering field 'missing'");
    }

    @ParameterizedTest
    @ValueSource(strings = {"blob", "embedding", "nested"})
    void testPayloadTypesCannotBeOrderingMetadata(String field) {
        RowType valueType =
                RowType.builder()
                        .field("blob", DataTypes.BLOB())
                        .field("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                        .field("nested", RowType.of(DataTypes.BLOB()))
                        .build();
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, field);
        assertThatThrownBy(() -> serializer(valueType, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot retain BLOB or VECTOR");
    }

    @Test
    void testManagedBlobCannotBeOrderingMetadata() {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "f0");
        options.set(CoreOptions.BLOB_FIELD, "f0");
        assertThatThrownBy(() -> serializer(RowType.of(DataTypes.BYTES()), options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLOB ordering field");
    }

    @Test
    void testRequiresPrimaryKey() {
        assertThatThrownBy(
                        () ->
                                new DeleteOrderingGuardSerializer(
                                        0,
                                        RowType.of(),
                                        RowType.of(),
                                        new CoreOptions(new Options())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("require primary keys");
    }

    private static DeleteOrderingGuardSerializer serializer(RowType valueType, Options options) {
        return new DeleteOrderingGuardSerializer(7, KEY_TYPE, valueType, new CoreOptions(options));
    }
}

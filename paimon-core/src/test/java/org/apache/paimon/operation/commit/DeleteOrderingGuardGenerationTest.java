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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.operation.commit.DeleteOrderingGuardGeneration.SNAPSHOT_PROPERTY;
import static org.apache.paimon.operation.commit.DeleteOrderingGuardGeneration.generation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests durable reset scopes independently of the lifetime of historical snapshot files. */
class DeleteOrderingGuardGenerationTest {

    private static final RowType PARTITION_TYPE =
            RowType.builder()
                    .field("pt", DataTypes.INT())
                    .field("region", DataTypes.STRING())
                    .build();
    private final InternalRowSerializer serializer = new InternalRowSerializer(PARTITION_TYPE);

    @Test
    void testPartialScopesCoverUnseenPartitionsAndRespectResetOrder() {
        Snapshot first = reset(null, Collections.singletonMap("pt", "1"));
        String firstGeneration = generation(first, partition(1, "unseen"));
        assertThat(firstGeneration).isNotEqualTo("0");
        assertThat(generation(first, partition(2, "unseen"))).isEqualTo("0");

        Map<String, String> narrowScope = new HashMap<>();
        narrowScope.put("pt", "1");
        narrowScope.put("region", "west");
        Snapshot second = reset(first, narrowScope);
        String westGeneration = generation(second, partition(1, "west"));
        assertThat(westGeneration).isNotEqualTo(firstGeneration);
        assertThat(generation(second, partition(1, "east"))).isEqualTo(firstGeneration);

        Snapshot third = reset(second, Collections.singletonMap("pt", "1"));
        assertThat(generation(third, partition(1, "west")))
                .isEqualTo(generation(third, partition(1, "east")))
                .isNotEqualTo(firstGeneration)
                .isNotEqualTo(westGeneration);
        assertThat(generation(third, partition(2, "west"))).isEqualTo("0");
    }

    @Test
    void testMultiplePartitionsIncludingNullValues() {
        PartitionPredicate filter =
                PartitionPredicate.fromMultiple(
                        PARTITION_TYPE, Arrays.asList(partition(1, "west"), partition(2, null)));
        Snapshot snapshot =
                snapshot(
                        DeleteOrderingGuardGeneration.merge(
                                null, null, true, true, filter, PARTITION_TYPE));
        assertThat(generation(snapshot, partition(1, "west")))
                .isEqualTo(generation(snapshot, partition(2, null)))
                .isNotEqualTo("0");
        assertThat(generation(snapshot, partition(1, "east"))).isEqualTo("0");
        assertThat(generation(snapshot, partition(2, "west"))).isEqualTo("0");
    }

    @Test
    void testInheritanceAfterCaptureDisabledAndFullReset() {
        Snapshot reset = reset(null, Collections.singletonMap("pt", "1"));
        Map<String, String> userProperties = new HashMap<>();
        userProperties.put("user-property", "retained");
        userProperties.put(SNAPSHOT_PROPERTY, "forged generation");
        Map<String, String> inherited =
                DeleteOrderingGuardGeneration.merge(
                        userProperties, reset, false, false, null, PARTITION_TYPE);
        assertThat(inherited)
                .containsEntry("user-property", "retained")
                .containsEntry(SNAPSHOT_PROPERTY, reset.properties().get(SNAPSHOT_PROPERTY));

        Snapshot truncated =
                snapshot(
                        DeleteOrderingGuardGeneration.merge(
                                null, snapshot(inherited), false, true, null, PARTITION_TYPE));
        assertThat(generation(truncated, partition(1, "west")))
                .isEqualTo(generation(truncated, partition(2, "west")))
                .isNotEqualTo(generation(reset, partition(1, "west")))
                .isNotEqualTo("0");
    }

    @Test
    void testDisabledTablesIgnoreUserSuppliedInternalState() {
        assertThat(DeleteOrderingGuardGeneration.forWriter(null, partition(1, "west"), false))
                .isNull();
        assertThat(DeleteOrderingGuardGeneration.forWriter(null, partition(1, "west"), true))
                .isEqualTo("0");
        assertThat(
                        DeleteOrderingGuardGeneration.merge(
                                Collections.singletonMap(SNAPSHOT_PROPERTY, "forged generation"),
                                null,
                                false,
                                false,
                                null,
                                PARTITION_TYPE))
                .isEmpty();
    }

    @Test
    void testUnknownGenerationFormatFailsClosed() {
        Snapshot snapshot =
                snapshot(Collections.singletonMap(SNAPSHOT_PROPERTY, "{\"version\":\"2\"}"));
        assertThatThrownBy(() -> generation(snapshot, partition(1, "west")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported delete ordering guard generation state");
    }

    private Snapshot reset(Snapshot previous, Map<String, String> scope) {
        return snapshot(
                DeleteOrderingGuardGeneration.merge(
                        null,
                        previous,
                        true,
                        true,
                        PartitionPredicate.fromMap(PARTITION_TYPE, scope, "__DEFAULT__"),
                        PARTITION_TYPE));
    }

    private Snapshot snapshot(Map<String, String> properties) {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.properties()).thenReturn(properties);
        return snapshot;
    }

    private BinaryRow partition(int pt, String region) {
        return serializer
                .toBinaryRow(
                        GenericRow.of(pt, region == null ? null : BinaryString.fromString(region)))
                .copy();
    }
}

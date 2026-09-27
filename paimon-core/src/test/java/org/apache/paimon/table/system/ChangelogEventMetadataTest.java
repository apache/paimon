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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for changelog event metadata row-type construction. */
class ChangelogEventMetadataTest {

    @Test
    void testMetadataUsesPhysicalFieldsWhenBaseRowPrependsSystemFields() {
        RowType valueType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "event_ts", DataTypes.BIGINT())));
        RowType baseRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(100, "rowkind", DataTypes.STRING()),
                                valueType.getField("id"),
                                valueType.getField("event_ts")));
        Options options = new Options();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        options.set(CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS, "event_ts");
        CoreOptions coreOptions = new CoreOptions(options);

        RowType extended =
                ChangelogEventMetadata.appendMetadataFields(baseRowType, valueType, coreOptions);

        assertThat(extended.getFieldNames())
                .containsExactly("rowkind", "id", "event_ts", "__internal__event_ts");
        assertThat(extended.getField("__internal__event_ts").type().isNullable()).isTrue();
        assertThat(extended.getField("__internal__event_ts").id()).isEqualTo(2);
    }

    @Test
    void testMetadataFieldIdsIncludeNestedFields() {
        RowType valueType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(
                                        1,
                                        "payload",
                                        DataTypes.ROW(new DataField(3, "nested", DataTypes.INT()))),
                                new DataField(2, "event_ts", DataTypes.BIGINT())));
        Options options = new Options();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        options.set(CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS, "event_ts");
        CoreOptions coreOptions = new CoreOptions(options);

        RowType extended =
                ChangelogEventMetadata.appendMetadataFields(valueType, valueType, coreOptions);

        assertThat(extended.getField("__internal__event_ts").id()).isEqualTo(4);
        assertThat(RowType.currentHighestFieldId(extended.getFields())).isEqualTo(4);
    }

    @Test
    void testStorageFieldIdentityIsStableAcrossColumnRename() {
        RowType originalValueType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "event_ts", DataTypes.BIGINT())));
        Options originalOptions = new Options();
        originalOptions.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        originalOptions.set(CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS, "event_ts");
        originalOptions.set(CoreOptions.CHANGELOG_PRODUCER_METADATA_FIELD_PREFIX, "__event__");

        RowType renamedValueType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "event_time", DataTypes.BIGINT())));
        Options renamedOptions = new Options();
        renamedOptions.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        renamedOptions.set(CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS, "event_time");
        renamedOptions.set(CoreOptions.CHANGELOG_PRODUCER_METADATA_FIELD_PREFIX, "__event__");

        DataField originalPublicField =
                ChangelogEventMetadata.extraValueFields(
                                originalValueType, new CoreOptions(originalOptions))
                        .get(0);
        DataField renamedPublicField =
                ChangelogEventMetadata.extraValueFields(
                                renamedValueType, new CoreOptions(renamedOptions))
                        .get(0);
        DataField originalStorageField =
                ChangelogEventMetadata.storageValueFields(
                                originalValueType, new CoreOptions(originalOptions))
                        .get(0);
        DataField renamedStorageField =
                ChangelogEventMetadata.storageValueFields(
                                renamedValueType, new CoreOptions(renamedOptions))
                        .get(0);

        assertThat(originalPublicField.name()).isEqualTo("__event__event_ts");
        assertThat(renamedPublicField.name()).isEqualTo("__event__event_time");
        assertThat(originalPublicField.id()).isEqualTo(renamedPublicField.id());
        assertThat(originalStorageField.name()).isEqualTo("__event__field_id_1");
        assertThat(renamedStorageField.name()).isEqualTo(originalStorageField.name());
        assertThat(renamedStorageField.id()).isEqualTo(originalStorageField.id());
    }
}

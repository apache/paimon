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

package org.apache.paimon.flink.sink;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Opt-in negotiation must retain before images without changing the default sink contract. */
class ChangelogAsAppendSinkTest {
    @Test
    void requestsBeforeImagesWhenEnabled() {
        assertThat(
                        sink(true, Collections.emptyMap(), false)
                                .getChangelogMode(ChangelogMode.upsert()))
                .isEqualTo(ChangelogMode.all());
    }

    @Test
    void leavesDefaultNegotiationUnchanged() {
        assertThat(
                        sink(false, Collections.emptyMap(), false)
                                .getChangelogMode(ChangelogMode.upsert()))
                .isEqualTo(ChangelogMode.upsert());
        assertThat(
                        sink(false, Collections.emptyMap(), false)
                                .getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void rejectsIncompatibleTableSettings() {
        for (Map<String, String> options :
                java.util.Arrays.asList(
                        Collections.singletonMap("ignore-delete", "true"),
                        Collections.singletonMap("rowkind.field", "kind"),
                        Collections.singletonMap("sink.key-only-deletes.enabled", "true"),
                        Collections.singletonMap(
                                "sink.changelog-as-append.time-field", "missing"))) {
            assertThatThrownBy(
                            () -> sink(true, options, false).getChangelogMode(ChangelogMode.all()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(
                        () ->
                                sink(true, Collections.emptyMap(), true)
                                        .getChangelogMode(ChangelogMode.all()))
                .isInstanceOf(IllegalArgumentException.class);
        FlinkTableSink sink = sink(true, Collections.emptyMap(), false);
        sink.applyOverwrite(true);
        assertThatThrownBy(() -> sink.getChangelogMode(ChangelogMode.all()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsFormatTables() {
        FormatTable table = mock(FormatTable.class);
        when(table.options())
                .thenReturn(Collections.singletonMap("sink.changelog-as-append", "true"));
        FlinkTableSink sink =
                new FlinkTableSink(ObjectIdentifier.of("catalog", "db", "log"), table, null);
        assertThatThrownBy(() -> sink.getChangelogMode(ChangelogMode.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-keyed FileStoreTable");
    }

    @Test
    void rejectsMetadataPartitionFields() {
        for (String field : new String[] {"kind", "emitted_ms"}) {
            FileStoreTable table = table(true, Collections.emptyMap(), false);
            when(table.partitionKeys()).thenReturn(Collections.singletonList(field));
            FlinkTableSink sink =
                    new FlinkTableSink(ObjectIdentifier.of("catalog", "db", "log"), table, null);
            assertThatThrownBy(() -> sink.getChangelogMode(ChangelogMode.all()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be partition fields");
        }
    }

    @Test
    void validatesOverwriteAtRuntimeProviderCreation() {
        FlinkTableSink sink = sink(true, Collections.emptyMap(), false);
        sink.getChangelogMode(ChangelogMode.all());
        sink.applyOverwrite(true);
        // Validation must run again if an ability is applied after changelog negotiation.
        assertThatThrownBy(() -> sink.getSinkRuntimeProvider(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overwrite");
    }

    private FlinkTableSink sink(boolean enabled, Map<String, String> extra, boolean keyed) {
        return new FlinkTableSink(
                ObjectIdentifier.of("catalog", "db", "log"), table(enabled, extra, keyed), null);
    }

    private FileStoreTable table(boolean enabled, Map<String, String> extra, boolean keyed) {
        FileStoreTable table = mock(FileStoreTable.class);
        Map<String, String> options = new HashMap<>();
        options.put("sink.changelog-as-append", Boolean.toString(enabled));
        options.put("sink.changelog-as-append.kind-field", "kind");
        options.put("sink.changelog-as-append.time-field", "emitted_ms");
        options.putAll(extra);
        when(table.options()).thenReturn(options);
        when(table.primaryKeys())
                .thenReturn(keyed ? Collections.singletonList("played") : Collections.emptyList());
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.rowType())
                .thenReturn(
                        RowType.of(
                                new DataType[] {
                                    new IntType(),
                                    new VarCharType(VarCharType.MAX_LENGTH),
                                    new BigIntType()
                                },
                                new String[] {"played", "kind", "emitted_ms"}));
        return table;
    }
}

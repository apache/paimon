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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSyncTableAction;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for single-table CDC schema initialization. */
class SyncTableActionBaseTest {

    @TempDir private Path warehouse;

    @ParameterizedTest
    @ValueSource(strings = {"", "__kafka_", "__custom_"})
    void testComputedColumnReferencesMetadata(String prefix) throws Exception {
        try (TestingKafkaSyncTableAction action = new TestingKafkaSyncTableAction(warehouse)) {
            action.withComputedColumnArgs(
                    Collections.singletonList(
                            "dt=date_format(" + prefix + "timestamp,yyyy-MM-dd)"));
            action.withMetadataColumnPrefix(prefix);
            action.withMetadataColumns(Collections.singletonList("timestamp"));
            action.withPartitionKeys("dt");

            // Create a new target, validate an existing target, then start with an empty topic.
            action.beforeBuildingSourceSink();
            action.beforeBuildingSourceSink();
            action.failSchemaRetrieval = true;
            action.beforeBuildingSourceSink();

            assertThat(action.fileStoreTable().schema().fields())
                    .extracting(DataField::name)
                    .containsExactly("id", "dt", prefix + "timestamp");
            assertThat(action.fileStoreTable().schema().partitionKeys()).containsExactly("dt");
            assertThat(action.fileStoreTable().primaryKeys()).isEmpty();
            assertThat(action.computedColumns).hasSize(1);
            assertThat(action.computedColumns.get(0).fieldReference())
                    .isEqualTo(prefix + "timestamp");
            assertThat(action.computedColumns.get(0).eval("2026-11-11 12:00:00.000"))
                    .isEqualTo("2026-11-11");
            assertThat(action.sourceSchema.fields())
                    .extracting(DataField::name)
                    .containsExactly("id");
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testMetadataComputedColumnsRejectedForNewPrimaryKeyTable(
            boolean inferPrimaryKey, boolean caseSensitive) throws Exception {
        try (TestingKafkaSyncTableAction action =
                new TestingKafkaSyncTableAction(warehouse, caseSensitive)) {
            if (inferPrimaryKey) {
                action.sourceSchema =
                        Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
            } else {
                action.withPrimaryKeys("id");
            }
            String reference = caseSensitive ? "__kafka_timestamp" : "__KAFKA_TIMESTAMP";
            action.withMetadataColumnPrefix("__kafka_");
            action.withMetadataColumns(Collections.singletonList("timestamp"));
            action.withComputedColumnArgs(
                    Arrays.asList(
                            "yr=substring(dt,0,4)",
                            "dt=date_format(" + reference + ",yyyy-MM-dd)"));

            assertThatThrownBy(action::beforeBuildingSourceSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Computed column 'dt'")
                    .hasMessageContaining("metadata column '" + reference + "'")
                    .hasMessageContaining("only supported for append-only tables");
            assertThat(action.tableExists()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMetadataComputedColumnsRejectedForExistingPrimaryKeyTable(boolean emptyTopic)
            throws Exception {
        try (TestingKafkaSyncTableAction action = new TestingKafkaSyncTableAction(warehouse)) {
            action.withPrimaryKeys("id");
            action.withMetadataColumns(Collections.singletonList("timestamp"));
            action.withComputedColumnArgs(Collections.singletonList("dt=substring(id,0)"));
            action.beforeBuildingSourceSink();
            assertThat(action.fileStoreTable().primaryKeys()).containsExactly("id");

            // Use the actual target keys even when neither the action nor source declares any.
            action.withPrimaryKeys(Collections.emptyList());
            action.withComputedColumnArgs(
                    Collections.singletonList("dt=date_format(timestamp,yyyy-MM-dd)"));
            action.failSchemaRetrieval = emptyTopic;

            assertThatThrownBy(action::beforeBuildingSourceSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Computed column 'dt'")
                    .hasMessageContaining("metadata column 'timestamp'")
                    .hasMessageContaining("only supported for append-only tables");
        }
    }

    @Test
    void testMetadataComputedColumnsAllowedWhenSourcePrimaryKeysAreDisabled() throws Exception {
        try (TestingKafkaSyncTableAction action = new TestingKafkaSyncTableAction(warehouse)) {
            action.sourceSchema =
                    Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
            action.syncPKeysFromSourceSchema(false);
            action.withMetadataColumns(Collections.singletonList("timestamp"));
            action.withComputedColumnArgs(
                    Collections.singletonList("dt=date_format(timestamp,yyyy-MM-dd)"));

            action.beforeBuildingSourceSink();

            assertThat(action.fileStoreTable().primaryKeys()).isEmpty();
            assertThat(action.computedColumns).hasSize(1);
        }
    }

    @Test
    void testUnconfiguredMetadataIsNotAvailableToComputedColumns() throws Exception {
        try (TestingKafkaSyncTableAction action = new TestingKafkaSyncTableAction(warehouse)) {
            action.withComputedColumnArgs(
                    Collections.singletonList("dt=date_format(__kafka_timestamp,yyyy-MM-dd)"));

            assertThatThrownBy(action::beforeBuildingSourceSink)
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Referenced field '__kafka_timestamp'");
        }
    }

    private static class TestingKafkaSyncTableAction extends KafkaSyncTableAction
            implements AutoCloseable {

        private Schema sourceSchema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        private boolean failSchemaRetrieval;

        private TestingKafkaSyncTableAction(Path warehouse) throws Exception {
            this(warehouse, true);
        }

        private TestingKafkaSyncTableAction(Path warehouse, boolean caseSensitive)
                throws Exception {
            super(
                    "test_db",
                    "test_table",
                    new Options()
                            .set(CatalogOptions.WAREHOUSE, warehouse.toString())
                            .set(CatalogOptions.CASE_SENSITIVE, caseSensitive)
                            .toMap(),
                    Collections.emptyMap());
            catalog.createDatabase(database, true);
            withTableConfig(Collections.singletonMap("bucket", "-1"));
        }

        @Override
        protected Schema retrieveSchema() throws SchemaRetrievalException {
            if (failSchemaRetrieval) {
                throw new SchemaRetrievalException("No records available");
            }
            return sourceSchema;
        }

        private boolean tableExists() {
            try {
                catalog.getTable(new Identifier(database, table));
                return true;
            } catch (Catalog.TableNotExistException e) {
                return false;
            }
        }

        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}

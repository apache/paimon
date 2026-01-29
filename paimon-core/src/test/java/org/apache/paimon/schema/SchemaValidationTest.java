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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.VECTOR_STORE_FIELDS;
import static org.apache.paimon.CoreOptions.VECTOR_STORE_FORMAT;
import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaValidationTest {

    private void validateTableSchemaExec(Map<String, String> options) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()),
                        new DataField(3, "f3", DataTypes.STRING()));
        List<String> partitionKeys = singletonList("f0");
        List<String> primaryKeys = singletonList("f1");
        options.put(BUCKET.key(), String.valueOf(-1));
        validateTableSchema(
                new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, ""));
    }

    private void validateBlobSchema(Map<String, String> options, List<String> partitions) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.BLOB()),
                        new DataField(3, "f3", DataTypes.STRING()));
        options.put(BUCKET.key(), String.valueOf(-1));
        validateTableSchema(new TableSchema(1, fields, 10, partitions, emptyList(), options, ""));
    }

    @Test
    public void testOnlyTimestampMillis() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_TIMESTAMP.toString());
        options.put(
                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        ThrowableAssert.ThrowingCallable validate = () -> validateTableSchemaExec(options);
        assertThatNoException().isThrownBy(validate);
    }

    @Test
    public void testOnlyTimestamp() {
        String timestampString =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_TIMESTAMP.toString());
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), timestampString);
        ThrowableAssert.ThrowingCallable validate = () -> validateTableSchemaExec(options);
        assertThatNoException().isThrownBy(validate);
    }

    @Test
    public void testFromTimestampConflict() {
        String timestampString =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_TIMESTAMP.toString());
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), timestampString);
        options.put(
                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(
                        "must set only one key in [scan.timestamp-millis,scan.timestamp] when you use from-timestamp for scan.mode");
    }

    @Test
    public void testFromSnapshotConflict() {
        String timestampString =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_TIMESTAMP.toString());
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), timestampString);
        options.put(SCAN_SNAPSHOT_ID.key(), String.valueOf(-1));
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(
                        "[scan.snapshot-id] must be null when you set [scan.timestamp-millis,scan.timestamp]");
    }

    @Test
    public void testRecordLevelTimeField() {
        Map<String, String> options = new HashMap<>(2);
        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "f0");
        options.put(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), "1 m");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();

        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "f10");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining("Can not find time field f10 for record level expire.");

        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "f3");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(
                        "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is STRING.");
    }

    @Test
    public void testBlobTableSchema() {
        Map<String, String> options = new HashMap<>();

        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        assertThatThrownBy(() -> validateBlobSchema(options, emptyList()))
                .hasMessage("Data evolution config must enabled for table with BLOB type column.");

        options.clear();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        assertThatThrownBy(() -> validateBlobSchema(options, singletonList("f2")))
                .hasMessage("The BLOB type column can not be part of partition keys.");

        assertThatThrownBy(
                        () -> {
                            validateTableSchema(
                                    new TableSchema(
                                            1,
                                            singletonList(new DataField(2, "f2", DataTypes.BLOB())),
                                            10,
                                            emptyList(),
                                            emptyList(),
                                            options,
                                            ""));
                        })
                .hasMessage("Table with BLOB type column must have other normal columns.");
    }

    @Test
    public void testPartialUpdateTableAggregateFunctionWithoutSequenceGroup() {
        Map<String, String> options = new HashMap<>(2);
        options.put("merge-engine", "partial-update");
        options.put("fields.f3.aggregate-function", "max");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(
                        "Must use sequence group for aggregation functions but not found for field");

        options.put("fields.f2.sequence-group", "f3");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();
    }

    @Test
    public void testChainTableAllowsNonDeduplicateMergeEngine() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CHAIN_TABLE_ENABLED.key(), "true");
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.SEQUENCE_FIELD.key(), "f2");
        options.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$f0");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd");
        options.put(CoreOptions.MERGE_ENGINE.key(), "partial-update");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.STRING()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.BIGINT()),
                        new DataField(3, "f3", DataTypes.STRING()));
        List<String> partitionKeys = singletonList("f0");
        List<String> primaryKeys = Arrays.asList("f0", "f1");
        TableSchema schema =
                new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, "");

        assertThatNoException().isThrownBy(() -> validateTableSchema(schema));
    }

    @Test
    public void testVectorStoreUnknownColumn() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_STORE_FORMAT.key(), "json");
        options.put(VECTOR_STORE_FIELDS.key(), "f99");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.STRING()));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")))
                .hasMessage("Some of the columns specified as vector-store are unknown.");
    }

    @Test
    public void testVectorStoreContainsBlobColumn() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_STORE_FORMAT.key(), "json");
        options.put(VECTOR_STORE_FIELDS.key(), "blob");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "blob", DataTypes.BLOB()));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")))
                .hasMessage("The vector-store columns can not be blob type.");
    }

    @Test
    public void testVectorStoreContainsPartitionColumn() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_STORE_FORMAT.key(), "json");
        options.put(VECTOR_STORE_FIELDS.key(), "f1");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.STRING()));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                singletonList("f1"),
                                                emptyList(),
                                                options,
                                                "")))
                .hasMessage("The vector-store columns can not be part of partition keys.");
    }

    @Test
    public void testVectorStoreRequireNormalColumns() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_STORE_FORMAT.key(), "json");
        options.put(VECTOR_STORE_FIELDS.key(), "f0,f1");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.STRING()));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")))
                .hasMessage("Table with vector-store must have other normal columns.");
    }

    @Test
    public void testVectorStoreRequiresDataEvolutionEnabled() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_STORE_FORMAT.key(), "json");
        options.put(VECTOR_STORE_FIELDS.key(), "f1");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.STRING()));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")))
                .hasMessage(
                        "Data evolution config must enabled for table with vector-store file format.");
    }
}

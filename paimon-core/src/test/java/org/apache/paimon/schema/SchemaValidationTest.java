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
import static org.apache.paimon.CoreOptions.VECTOR_FIELD;
import static org.apache.paimon.CoreOptions.VECTOR_FILE_FORMAT;
import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
import static org.apache.paimon.schema.TableSchema.CURRENT_VERSION;
import static org.apache.paimon.schema.TableSchema.PAIMON_07_VERSION;
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
    public void testNestedRowAggregateOptionRejected() {
        // PAIMON-6471: nested-path agg config was silently ignored, leaving the parent ROW
        // on the default aggregator and producing wrong results without any error.
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1,
                                "data",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "num", DataTypes.INT()),
                                        DataTypes.FIELD(3, "info", DataTypes.STRING()))));

        Map<String, String> options = new HashMap<>();
        options.put("merge-engine", "aggregation");
        options.put("fields.data.num.aggregate-function", "sum");
        options.put("fields.data.sequence-group", "data");
        options.put(BUCKET.key(), "1");

        TableSchema schema =
                new TableSchema(1, fields, 10, emptyList(), singletonList("id"), options, "");

        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessageContaining("Nested-field path is not supported on ROW field 'data'")
                .hasMessageContaining("fields.data.num.aggregate-function");
    }

    @Test
    public void testMapStorageLayout() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(
                                1, "metrics", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        new DataField(
                                2, "codes", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())));

        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), "-1");
        options.put("fields.metrics.map.storage-layout", "shared-shredding");
        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")));

        options.put("fields.metrics.map.storage-layout", "default");
        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")));

        options.put("fields.nonexist.map.storage-layout", "shared-shredding");
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
                .hasMessageContaining("Field nonexist can not be found in table schema.");

        options.remove("fields.nonexist.map.storage-layout");
        options.put("fields.id.map.storage-layout", "default");
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
                .hasMessageContaining(
                        "Column 'id' is configured with map.storage-layout but its type is not MAP.");

        options.remove("fields.id.map.storage-layout");
        options.put("fields.codes.map.storage-layout", "shared-shredding");
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
                .hasMessageContaining(
                        "Column 'codes' is configured with map.storage-layout=shared-shredding but its type is not MAP<STRING, T>.");

        options.remove("fields.codes.map.storage-layout");
        options.put("fields.metrics.map.storage-layout", "shared-shredding");
        options.put("fields.metrics.map.shared-shredding.max-columns", "0");
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
                .hasMessageContaining("options map.shared-shredding.max-columns must > 0");
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
    public void testChainTableRequiresSequenceField() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CHAIN_TABLE_ENABLED.key(), "true");
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$f0");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd");

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

        assertThatThrownBy(() -> validateTableSchema(schema))
                .hasMessage("Sequence field is required for chain table.");
    }

    @Test
    public void testVectorStoreUnknownColumn() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_FILE_FORMAT.key(), "json");
        options.put(VECTOR_FIELD.key(), "f99");

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
                .hasMessage("Some of the columns specified as vector-field are unknown.");
    }

    @Test
    public void testVectorStoreContainsNonVectorColumn() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_FILE_FORMAT.key(), "json");
        options.put(VECTOR_FIELD.key(), "f1");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.FLOAT()));
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
                        "Field name[f1] is configured as vector-field so the type must be vector, but it is FLOAT");
    }

    @Test
    public void testVectorStoreContainsPartitionColumn() {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_FILE_FORMAT.key(), "json");
        options.put(VECTOR_FIELD.key(), "f1");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.VECTOR(6, DataTypes.FLOAT())));
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
                .hasMessage("The type VectorType in partition field f1 is unsupported");
    }

    @Test
    public void testVectorStoreRequiresDataEvolutionEnabled() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(VECTOR_FILE_FORMAT.key(), "json");
        options.put(VECTOR_FIELD.key(), "f1");

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.VECTOR(6, DataTypes.FLOAT())));
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

    @Test
    public void testVectorTypeCanNotBeKey() {
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        vectorTypeSchema(emptyList(), singletonList("f1"), null)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "The type %s in primary key field %s is unsupported", "VectorType", "f1");

        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        vectorTypeSchema(singletonList("f1"), emptyList(), null)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("The type %s in partition field %s is unsupported", "VectorType", "f1");

        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        vectorTypeSchema(
                                                emptyList(), emptyList(), singletonList("f1"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "The type %s in upsert key field %s is unsupported", "VectorType", "f1");
    }

    @Test
    void testRowTrackingWithPkTable() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(BUCKET.key(), String.valueOf(-2));

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.STRING()));
        List<String> primaryKeys = singletonList("f1");

        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                primaryKeys,
                                                options,
                                                "")))
                .hasMessageContaining("primary-key");
    }

    @Test
    public void testFileIndexColumns() {
        List<String> keys =
                Arrays.asList(
                        "file-index.bloom-filter.columns",
                        "file-index.bitmap.columns",
                        "file-index.bsi.columns",
                        "file-index.range-bitmap.columns");

        for (String key : keys) {
            // valid: all referenced columns exist and types are supported
            Map<String, String> okOptions = new HashMap<>();
            okOptions.put(key, "f0");
            assertThatCode(() -> validateTableSchemaExec(okOptions))
                    .as("valid key=%s", key)
                    .doesNotThrowAnyException();

            // invalid: references a non-existent column
            Map<String, String> badOptions = new HashMap<>();
            badOptions.put(key, "f0,not_exist");
            assertThatThrownBy(() -> validateTableSchemaExec(badOptions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Column 'not_exist' specified in 'file-index.<index-type>.columns' does not exist in table schema.");
        }
    }

    @Test
    public void testFileIndexColumnOptionWithoutColumnsDeclaration() {
        Map<String, String> options = new HashMap<>();
        options.put("file-index.bloom-filter.vin.items", "2000");
        options.put("file-index.bloom-filter.vin.fpp", "0.0001");

        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Wrong file index option 'file-index.bloom-filter.vin.")
                .hasMessageContaining(
                        "column 'vin' is not declared in 'file-index.bloom-filter.columns'")
                .hasMessageContaining("Please add 'file-index.bloom-filter.columns' = 'vin'")
                .hasMessageContaining(
                        "For map nested index, declare it like '<map-column>[<map-key>]'");
    }

    @Test
    public void testFileIndexNestedColumn() {
        List<String> keys =
                Arrays.asList(
                        "file-index.bloom-filter.columns",
                        "file-index.bitmap.columns",
                        "file-index.bsi.columns",
                        "file-index.range-bitmap.columns");

        for (String key : keys) {
            // valid: nested syntax on a map column with string key
            Map<String, String> okOptions = new HashMap<>();
            okOptions.put(key, "m[k]");
            assertThatCode(() -> validateTableSchemaWithMapField(okOptions))
                    .doesNotThrowAnyException();

            // invalid: nested syntax on a non-map column
            Map<String, String> nonMapOptions = new HashMap<>();
            nonMapOptions.put(key, "f3[k]");
            assertThatThrownBy(() -> validateTableSchemaWithMapField(nonMapOptions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Column 'f3' is configured as nested column in 'file-index.<index-type>.columns' but is not a map type.");

            // invalid: nested syntax on a map column with non-string key
            Map<String, String> nonStringKeyOptions = new HashMap<>();
            nonStringKeyOptions.put(key, "mi[k]");
            assertThatThrownBy(() -> validateTableSchemaWithMapField(nonStringKeyOptions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Column 'mi' is configured as nested column in 'file-index.<index-type>.columns', but its map key type is INT. Only CHAR/VARCHAR/STRING is supported.");
        }
    }

    private void validateTableSchemaWithMapField(Map<String, String> options) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()),
                        new DataField(3, "f3", DataTypes.STRING()),
                        new DataField(4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        new DataField(5, "mi", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())));
        options.put(BUCKET.key(), String.valueOf(-1));
        validateTableSchema(
                new TableSchema(1, fields, 10, emptyList(), singletonList("f1"), options, ""));
    }

    @Test
    public void testFileIndexUnsupportedDataType() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "arr", DataTypes.ARRAY(DataTypes.STRING())),
                        new DataField(3, "f3", DataTypes.STRING()));

        // bloom-filter on ARRAY should fail
        Map<String, String> bloomOptions = new HashMap<>();
        bloomOptions.put("file-index.bloom-filter.columns", "arr");
        bloomOptions.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                singletonList("f1"),
                                                bloomOptions,
                                                "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported by 'bloom-filter' index");

        // bitmap on ARRAY should fail
        Map<String, String> bitmapOptions = new HashMap<>();
        bitmapOptions.put("file-index.bitmap.columns", "arr");
        bitmapOptions.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                singletonList("f1"),
                                                bitmapOptions,
                                                "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported by 'bitmap' index");

        // bsi on STRING should fail
        Map<String, String> bsiOptions = new HashMap<>();
        bsiOptions.put("file-index.bsi.columns", "f3");
        bsiOptions.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                singletonList("f1"),
                                                bsiOptions,
                                                "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported by 'bsi' index");

        // bloom-filter on INT should pass
        Map<String, String> okOptions = new HashMap<>();
        okOptions.put("file-index.bloom-filter.columns", "f0");
        okOptions.put(BUCKET.key(), String.valueOf(-1));
        assertThatCode(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                singletonList("f1"),
                                                okOptions,
                                                "")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testSnapshotSequenceOrderingHappyPath() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        assertThatNoException().isThrownBy(() -> validateTableSchemaExec(options));
    }

    @Test
    public void testSnapshotSequenceOrderingRejectsNonWriteOnly() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(CoreOptions.WRITE_ONLY.key());
    }

    @Test
    public void testSnapshotSequenceOrderingRejectsSequenceField() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.SEQUENCE_FIELD.key(), "f2");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining("sequence.field");
    }

    @Test
    public void testSnapshotSequenceOrderingRejectsNonPkTable() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        options.put(BUCKET.key(), String.valueOf(-1));
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
                .hasMessageContaining("primary-key");
    }

    @Test
    public void testFileFormatPerLevelRejectsIncompatibleSchema() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "k", DataTypes.INT()),
                        new DataField(1, "v", DataTypes.TIMESTAMP(9)));
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.FILE_FORMAT_PER_LEVEL.key(), "0:avro");

        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                singletonList("k"),
                                                options,
                                                "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file.format.per.level")
                .hasMessageContaining("0:avro")
                .hasMessageContaining("TIMESTAMP");
    }

    @Test
    public void testFileFormatPerLevelAcceptsCompatibleSchema() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "k", DataTypes.INT()),
                        new DataField(1, "v", DataTypes.TIMESTAMP(9)));
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        options.put(CoreOptions.FILE_FORMAT_PER_LEVEL.key(), "0:parquet");

        validateTableSchema(
                new TableSchema(1, fields, 10, emptyList(), singletonList("k"), options, ""));
    }

    @Test
    void testManifestSortValidation() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()));

        // Test 1: manifest-sort.enabled on non-partition table should fail
        Map<String, String> options1 = new HashMap<>();
        options1.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "true");
        options1.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options1,
                                                "")))
                .hasMessageContaining(
                        "Cannot enable 'manifest-sort.enabled' for non-partition table.");

        // Test 2: manifest-sort-partition-field not in partition keys should fail
        Map<String, String> options2 = new HashMap<>();
        options2.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "true");
        options2.put(CoreOptions.MANIFEST_SORT_PARTITION_FIELD.key(), "f1");
        options2.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                singletonList("f0"),
                                                emptyList(),
                                                options2,
                                                "")))
                .hasMessageContaining("is not a partition field");

        // Test 3: valid manifest-sort config should pass
        Map<String, String> options3 = new HashMap<>();
        options3.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "true");
        options3.put(CoreOptions.MANIFEST_SORT_PARTITION_FIELD.key(), "f0");
        options3.put(BUCKET.key(), String.valueOf(-1));
        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                singletonList("f0"),
                                                emptyList(),
                                                options3,
                                                "")));

        // Test 4: data evolution tables can sort manifests by RowID without partition keys
        Map<String, String> options4 = new HashMap<>();
        options4.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "true");
        options4.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options4.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options4.put(BUCKET.key(), String.valueOf(-1));
        assertThatNoException()
                .isThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options4,
                                                "")));

        // Test 5: data evolution tables should still validate configured partition field
        Map<String, String> options5 = new HashMap<>();
        options5.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "true");
        options5.put(CoreOptions.MANIFEST_SORT_PARTITION_FIELD.key(), "f1");
        options5.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options5.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options5.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                singletonList("f0"),
                                                emptyList(),
                                                options5,
                                                "")))
                .hasMessageContaining("is not a partition field");

        // Test 6: data evolution non-partition tables cannot configure a partition field
        Map<String, String> options6 = new HashMap<>();
        options6.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "true");
        options6.put(CoreOptions.MANIFEST_SORT_PARTITION_FIELD.key(), "f0");
        options6.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options6.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options6.put(BUCKET.key(), String.valueOf(-1));
        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options6,
                                                "")))
                .hasMessageContaining("is not a partition field");
    }

    @Test
    public void testMergeOnReadCoexistsWithVisibilityCallback() {
        Map<String, String> options = new HashMap<>();
        options.put("deletion-vectors.enabled", "true");
        options.put("deletion-vectors.merge-on-read", "true");
        options.put("visibility-callback.enabled", "true");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();
    }

    @Test
    public void testMergeOnReadCoexistsWithVisibilityCallbackAndPostponeBucket() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()),
                        new DataField(3, "f3", DataTypes.STRING()));
        Map<String, String> options = new HashMap<>();
        options.put("deletion-vectors.enabled", "true");
        options.put("deletion-vectors.merge-on-read", "true");
        options.put("visibility-callback.enabled", "true");
        options.put(BUCKET.key(), String.valueOf(-2));
        assertThatCode(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                singletonList("f0"),
                                                singletonList("f1"),
                                                options,
                                                "")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testBucketAppendBackwardCompatibility() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.STRING()));

        Map<String, String> legacyOptions = new HashMap<>();
        legacyOptions.put(BUCKET.key(), "1");

        TableSchema legacySchema =
                new TableSchema(
                        PAIMON_07_VERSION,
                        0L,
                        fields,
                        1,
                        emptyList(),
                        emptyList(),
                        legacyOptions,
                        "",
                        0L);

        assertThatCode(() -> validateTableSchema(legacySchema)).doesNotThrowAnyException();

        Map<String, String> currentOptions = new HashMap<>();
        currentOptions.put(BUCKET.key(), "1");

        TableSchema currentSchema =
                new TableSchema(
                        CURRENT_VERSION,
                        0L,
                        fields,
                        1,
                        emptyList(),
                        emptyList(),
                        currentOptions,
                        "",
                        0L);

        assertThatThrownBy(() -> validateTableSchema(currentSchema))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("bucket-key");

        Map<String, String> legacyMultiBucketOptions = new HashMap<>();
        legacyMultiBucketOptions.put(BUCKET.key(), "2");

        TableSchema legacyMultiBucketSchema =
                new TableSchema(
                        PAIMON_07_VERSION,
                        0L,
                        fields,
                        1,
                        emptyList(),
                        emptyList(),
                        legacyMultiBucketOptions,
                        "",
                        0L);

        assertThatThrownBy(() -> validateTableSchema(legacyMultiBucketSchema))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("bucket-key");
    }

    @Test
    public void testMergeOnReadRequiresDvEnabled() {
        Map<String, String> options = new HashMap<>();
        options.put("deletion-vectors.merge-on-read", "true");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(
                        "deletion-vectors.merge-on-read requires deletion-vectors.enabled to be true");
    }

    @Test
    public void testFullCompactionDeltaCommitsWithLookupChangelogProducer() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");
        options.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key())
                .hasMessageContaining("lookup")
                .hasMessageContaining("full-compaction");

        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();

        options.clear();
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();

        options.clear();
        options.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "input");
        assertThatCode(() -> validateTableSchemaExec(options)).doesNotThrowAnyException();
    }

    private TableSchema vectorTypeSchema(
            List<String> partitionKeys, List<String> primaryKeys, List<String> upsertKeys) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), String.valueOf(-1));
        if (upsertKeys != null) {
            options.put(CoreOptions.UPSERT_KEY.key(), String.join(",", upsertKeys));
        }
        return new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, "");
    }
}

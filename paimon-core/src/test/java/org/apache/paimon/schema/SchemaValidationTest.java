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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
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
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Collections.singletonList("f1");
        options.put(BUCKET.key(), String.valueOf(-1));
        validateTableSchema(
                new TableSchema(1, fields, 10, partitionKeys, primaryKeys, options, ""));
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
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "f0");
        options.put(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), "1 m");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining("Time field f0 for record-level expire should be not null");

        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "f10");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining("Can not find time field f10 for record level expire.");

        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "f3");
        assertThatThrownBy(() -> validateTableSchemaExec(options))
                .hasMessageContaining(
                        "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is STRING.");
    }
}

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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.schema.Schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.buildPaimonSchema;

/** Build schema for new table found in database synchronization. */
public class NewTableSchemaBuilder implements Serializable {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;
    private final CdcMetadataConverter[] metadataConverters;

    public NewTableSchemaBuilder(
            Map<String, String> tableConfig,
            boolean caseSensitive,
            List<String> partitionKeys,
            List<String> primaryKeys,
            CdcMetadataConverter[] metadataConverters) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
        this.metadataConverters = metadataConverters;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
    }

    public Optional<Schema> build(RichCdcMultiplexRecord record) {
        Schema sourceSchema =
                new Schema(
                        record.fields(),
                        Collections.emptyList(),
                        record.primaryKeys(),
                        Collections.emptyMap(),
                        null);
        return Optional.of(
                buildPaimonSchema(
                        record.tableName(),
                        partitionKeys,
                        primaryKeys,
                        Collections.emptyList(),
                        tableConfig,
                        sourceSchema,
                        metadataConverters,
                        caseSensitive,
                        false,
                        true));
    }
}

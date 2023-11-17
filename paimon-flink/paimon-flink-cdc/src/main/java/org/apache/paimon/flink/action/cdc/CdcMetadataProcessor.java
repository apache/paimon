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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Enumerates the metadata processing behaviors for CDC related data.
 *
 * <p>This enumeration provides definitions for various CDC metadata keys along with their
 * associated data types and converters. Each enum entry represents a specific type of metadata
 * related to CDC and provides a mechanism to read and process this metadata from a given {@link
 * JsonNode} source.
 *
 * <p>The provided converters, which are of type {@link CdcMetadataConverter}, define how the raw
 * metadata is transformed or processed for each specific metadata key.
 */
public enum CdcMetadataProcessor {
    MYSQL_METADATA_PROCESSOR(
            SyncJobHandler.SourceType.MYSQL,
            new CdcMetadataConverter.DatabaseNameConverter(),
            new CdcMetadataConverter.TableNameConverter(),
            new CdcMetadataConverter.OpTsConverter()),
    POSTGRES_METADATA_PROCESSOR(
            SyncJobHandler.SourceType.POSTGRES,
            new CdcMetadataConverter.DatabaseNameConverter(),
            new CdcMetadataConverter.TableNameConverter(),
            new CdcMetadataConverter.SchemaNameConverter(),
            new CdcMetadataConverter.OpTsConverter()),

    SQLSERVER_METADATA_PROCESSOR(
            SyncJobHandler.SourceType.SQLSERVER,
            new CdcMetadataConverter.DatabaseNameConverter(),
            new CdcMetadataConverter.TableNameConverter(),
            new CdcMetadataConverter.SchemaNameConverter(),
            new CdcMetadataConverter.OpTsConverter());

    private final SyncJobHandler.SourceType sourceType;

    private final CdcMetadataConverter[] cdcMetadataConverters;

    CdcMetadataProcessor(
            SyncJobHandler.SourceType sourceType, CdcMetadataConverter... cdcMetadataConverters) {
        this.sourceType = sourceType;
        this.cdcMetadataConverters = cdcMetadataConverters;
    }

    private static final Map<SyncJobHandler.SourceType, Map<String, CdcMetadataConverter>>
            METADATA_CONVERTERS =
                    Arrays.stream(CdcMetadataProcessor.values())
                            .collect(
                                    Collectors.toMap(
                                            CdcMetadataProcessor::sourceType,
                                            value ->
                                                    Arrays.stream(value.cdcMetadataConverters())
                                                            .collect(
                                                                    Collectors.toMap(
                                                                            CdcMetadataConverter
                                                                                    ::columnName,
                                                                            Function.identity()))));

    public static CdcMetadataConverter converter(
            SyncJobHandler.SourceType sourceType, String column) {
        return checkNotNull(checkNotNull(METADATA_CONVERTERS.get(sourceType)).get(column));
    }

    private CdcMetadataConverter[] cdcMetadataConverters() {
        return cdcMetadataConverters;
    }

    private SyncJobHandler.SourceType sourceType() {
        return sourceType;
    }
}

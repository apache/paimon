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

import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

/**
 * Wraps a {@link CdcMetadataConverter} to add a prefix to its column name.
 *
 * <p>This decorator allows adding prefixes like "__kafka_" to metadata column names to avoid
 * collisions with source data columns, while keeping the underlying converter logic unchanged.
 */
public class PrefixedMetadataConverter implements CdcMetadataConverter {

    private static final long serialVersionUID = 1L;

    private final CdcMetadataConverter delegate;
    private final String prefix;

    public PrefixedMetadataConverter(CdcMetadataConverter delegate, String prefix) {
        this.delegate = delegate;
        this.prefix = prefix != null ? prefix : "";
    }

    @Override
    public String columnName() {
        return prefix + delegate.columnName();
    }

    @Override
    public String read(JsonNode payload) {
        return delegate.read(payload);
    }

    @Override
    public String read(CdcSourceRecord record) {
        return delegate.read(record);
    }

    @Override
    public DataType dataType() {
        return delegate.dataType();
    }
}

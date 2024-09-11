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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Sort order in Iceberg's metadata.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#sort-orders">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergSortOrder {

    // currently unsupported
    public static final int ORDER_ID = 0;

    private static final String FIELD_ORDER_ID = "order-id";
    private static final String FIELD_FIELDS = "fields";

    @JsonProperty(FIELD_ORDER_ID)
    private final int orderId;

    // currently always empty
    @JsonProperty(FIELD_FIELDS)
    private final List<Object> fields;

    public IcebergSortOrder() {
        this(ORDER_ID, new ArrayList<>());
    }

    @JsonCreator
    public IcebergSortOrder(
            @JsonProperty(FIELD_ORDER_ID) int orderId,
            @JsonProperty(FIELD_FIELDS) List<Object> fields) {
        this.orderId = orderId;
        this.fields = fields;
    }

    @JsonGetter(FIELD_ORDER_ID)
    public int orderId() {
        return orderId;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<Object> fields() {
        return fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSortOrder)) {
            return false;
        }

        IcebergSortOrder that = (IcebergSortOrder) o;
        return orderId == that.orderId && Objects.equals(fields, that.fields);
    }
}

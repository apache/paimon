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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A data change record from the CDC source. */
public class CdcSourceRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String topic;

    @Nullable private final Object key;

    // TODO Use generics to support more scenarios.
    private final Object value;

    // Generic metadata map - any source can add metadata
    private final Map<String, Object> metadata;

    public CdcSourceRecord(@Nullable String topic, @Nullable Object key, Object value) {
        this(topic, key, value, null);
    }

    public CdcSourceRecord(Object value) {
        this(null, null, value, null);
    }

    public CdcSourceRecord(
            @Nullable String topic,
            @Nullable Object key,
            Object value,
            @Nullable Map<String, Object> metadata) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.metadata =
                metadata != null
                        ? Collections.unmodifiableMap(new HashMap<>(metadata))
                        : Collections.emptyMap();
    }

    @Nullable
    public String getTopic() {
        return topic;
    }

    @Nullable
    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Nullable
    public Object getMetadata(String key) {
        return metadata.get(key);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CdcSourceRecord)) {
            return false;
        }

        CdcSourceRecord that = (CdcSourceRecord) o;
        return Objects.equals(topic, that.topic)
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, key, value, metadata);
    }

    @Override
    public String toString() {
        return topic + ": " + key + " " + value;
    }
}

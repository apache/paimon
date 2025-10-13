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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Snapshot summary in Iceberg's snapshot.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#snapshots">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergSnapshotSummary {

    private static final String FIELD_OPERATION = "operation";

    public static final IcebergSnapshotSummary APPEND = new IcebergSnapshotSummary("append");
    public static final IcebergSnapshotSummary OVERWRITE = new IcebergSnapshotSummary("overwrite");

    private final Map<String, String> summary;

    @JsonCreator
    public IcebergSnapshotSummary(Map<String, String> summary) {
        this.summary = summary != null ? new HashMap<>(summary) : new HashMap<>();
    }

    public IcebergSnapshotSummary(String operation) {
        this.summary = new HashMap<>();
        this.summary.put(FIELD_OPERATION, operation);
    }

    @JsonValue
    public Map<String, String> getSummary() {
        return new HashMap<>(summary);
    }

    public String operation() {
        return summary.get(FIELD_OPERATION);
    }

    public String get(String key) {
        return summary.get(key);
    }

    public void put(String key, String value) {
        summary.put(key, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summary);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSnapshotSummary)) {
            return false;
        }

        IcebergSnapshotSummary that = (IcebergSnapshotSummary) o;
        return Objects.equals(summary, that.summary);
    }
}

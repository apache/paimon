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

    @JsonProperty(FIELD_OPERATION)
    private final String operation;

    @JsonCreator
    public IcebergSnapshotSummary(@JsonProperty(FIELD_OPERATION) String operation) {
        this.operation = operation;
    }

    @JsonGetter(FIELD_OPERATION)
    public String operation() {
        return operation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation);
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
        return Objects.equals(operation, that.operation);
    }
}

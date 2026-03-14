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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Iceberg's ref metadata.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#snapshot-references">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergRef {

    private static final String FIELD_SNAPSHOT_ID = "snapshot-id";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_MAX_REF_AGE_MS = "max-ref-age-ms";

    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final long snapshotId;

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_MAX_REF_AGE_MS)
    @Nullable
    private final Long maxRefAgeMs;

    @JsonCreator
    public IcebergRef(@JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId) {
        this.snapshotId = snapshotId;
        this.type = "tag"; // Only type supported is tag
        this.maxRefAgeMs =
                Long.MAX_VALUE; // Tags are expired by Paimon, not by Iceberg compatibility. So
        // this value is set to a default value of Long.MAX_VALUE.
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    public long snapshotId() {
        return snapshotId;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_MAX_REF_AGE_MS)
    public Long maxRefAgeMs() {
        return maxRefAgeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergRef)) {
            return false;
        }
        IcebergRef that = (IcebergRef) o;
        return snapshotId == that.snapshotId
                && type.equals(that.type)
                && maxRefAgeMs == that.maxRefAgeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, type, maxRefAgeMs);
    }
}

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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Filter used by {@code Catalog#listSchemas} to express single-endpoint schema queries.
 *
 * <p>All schema read patterns (latest / earliest / by-id / by-range / all) share the same catalog
 * method and are distinguished by which fields of this filter are populated. At most one of {@link
 * #isLatest()}, {@link #isEarliest()}, {@link #schemaId()} may be set; when none of them is set,
 * {@link #maxSchemaId()} / {@link #minSchemaId()} may optionally restrict the returned range.
 */
public class SchemaFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final SchemaFilter ALL = new SchemaFilter(false, false, null, null, null);
    private static final SchemaFilter LATEST = new SchemaFilter(true, false, null, null, null);
    private static final SchemaFilter EARLIEST = new SchemaFilter(false, true, null, null, null);

    private final boolean latest;
    private final boolean earliest;
    @Nullable private final Long schemaId;
    @Nullable private final Long maxSchemaId;
    @Nullable private final Long minSchemaId;

    private SchemaFilter(
            boolean latest,
            boolean earliest,
            @Nullable Long schemaId,
            @Nullable Long maxSchemaId,
            @Nullable Long minSchemaId) {
        int exclusive = 0;
        if (latest) {
            exclusive++;
        }
        if (earliest) {
            exclusive++;
        }
        if (schemaId != null) {
            exclusive++;
        }
        checkArgument(
                exclusive <= 1,
                "SchemaFilter is over-constrained: latest / earliest / schemaId are mutually exclusive.");
        if (exclusive == 1) {
            checkArgument(
                    maxSchemaId == null && minSchemaId == null,
                    "SchemaFilter is over-constrained: range cannot be combined with latest / earliest / schemaId.");
        }
        this.latest = latest;
        this.earliest = earliest;
        this.schemaId = schemaId;
        this.maxSchemaId = maxSchemaId;
        this.minSchemaId = minSchemaId;
    }

    public static SchemaFilter all() {
        return ALL;
    }

    public static SchemaFilter latest() {
        return LATEST;
    }

    public static SchemaFilter earliest() {
        return EARLIEST;
    }

    public static SchemaFilter withId(long schemaId) {
        return new SchemaFilter(false, false, schemaId, null, null);
    }

    public static SchemaFilter range(@Nullable Long maxSchemaId, @Nullable Long minSchemaId) {
        return new SchemaFilter(false, false, null, maxSchemaId, minSchemaId);
    }

    public boolean isLatest() {
        return latest;
    }

    public boolean isEarliest() {
        return earliest;
    }

    @Nullable
    public Long schemaId() {
        return schemaId;
    }

    @Nullable
    public Long maxSchemaId() {
        return maxSchemaId;
    }

    @Nullable
    public Long minSchemaId() {
        return minSchemaId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaFilter)) {
            return false;
        }
        SchemaFilter that = (SchemaFilter) o;
        return latest == that.latest
                && earliest == that.earliest
                && Objects.equals(schemaId, that.schemaId)
                && Objects.equals(maxSchemaId, that.maxSchemaId)
                && Objects.equals(minSchemaId, that.minSchemaId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latest, earliest, schemaId, maxSchemaId, minSchemaId);
    }

    @Override
    public String toString() {
        return "SchemaFilter{"
                + "latest="
                + latest
                + ", earliest="
                + earliest
                + ", schemaId="
                + schemaId
                + ", maxSchemaId="
                + maxSchemaId
                + ", minSchemaId="
                + minSchemaId
                + '}';
    }
}

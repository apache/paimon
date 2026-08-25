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

package org.apache.paimon.management;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Stable reference to a permission principal. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PrincipalRef {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_ID = "id";

    @JsonProperty(FIELD_TYPE)
    private final PrincipalType type;

    @JsonProperty(FIELD_ID)
    private final String id;

    @JsonCreator
    @ConstructorProperties({FIELD_TYPE, FIELD_ID})
    public PrincipalRef(@JsonProperty(FIELD_TYPE) String type, @JsonProperty(FIELD_ID) String id) {
        this(PrincipalType.fromString(type), id);
    }

    public PrincipalRef(PrincipalType type, String id) {
        this.type = checkNotNull(type, "principal type cannot be null");
        checkArgument(!isBlank(id), "principal id cannot be empty.");
        this.id = id;
    }

    @JsonGetter(FIELD_TYPE)
    public PrincipalType getType() {
        return type;
    }

    @JsonGetter(FIELD_ID)
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrincipalRef)) {
            return false;
        }
        PrincipalRef that = (PrincipalRef) o;
        return type == that.type && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

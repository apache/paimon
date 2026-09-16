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

package org.apache.paimon.rest;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A named database-level branch or immutable tag. */
@Experimental
public class DatabaseReference {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_NAME = "name";
    private static final String NAME_PATTERN = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}";

    @JsonProperty(FIELD_TYPE)
    private final DatabaseReferenceType type;

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonCreator
    @ConstructorProperties({FIELD_TYPE, FIELD_NAME})
    public DatabaseReference(
            @JsonProperty(FIELD_TYPE) DatabaseReferenceType type,
            @JsonProperty(FIELD_NAME) String name) {
        checkArgument(type != null, "Reference type must not be null");
        validateName(name);
        this.type = type;
        this.name = name;
    }

    static void validateName(String name) {
        checkArgument(
                name != null && name.matches(NAME_PATTERN), "Invalid reference name: %s", name);
    }

    @JsonGetter(FIELD_TYPE)
    public DatabaseReferenceType getType() {
        return type;
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DatabaseReference)) {
            return false;
        }
        DatabaseReference that = (DatabaseReference) o;
        return type == that.type && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        return type + ":" + name;
    }
}

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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/** An exact table or view participating in a dependency read. */
@Public
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ReadAuthorizationResource implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_IDENTIFIER = "identifier";

    private final ReadAuthorizationRootType type;
    private final Identifier identifier;

    @JsonCreator
    public ReadAuthorizationResource(
            @JsonProperty(FIELD_TYPE) ReadAuthorizationRootType type,
            @JsonProperty(FIELD_IDENTIFIER) Identifier identifier) {
        this.type = Objects.requireNonNull(type, "type");
        this.identifier = Objects.requireNonNull(identifier, "identifier");
    }

    public static ReadAuthorizationResource table(Identifier identifier) {
        return new ReadAuthorizationResource(ReadAuthorizationRootType.TABLE, identifier);
    }

    public static ReadAuthorizationResource view(Identifier identifier) {
        return new ReadAuthorizationResource(ReadAuthorizationRootType.VIEW, identifier);
    }

    @JsonGetter(FIELD_TYPE)
    public ReadAuthorizationRootType type() {
        return type;
    }

    @JsonGetter(FIELD_IDENTIFIER)
    public Identifier identifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadAuthorizationResource that = (ReadAuthorizationResource) o;
        return type == that.type && identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, identifier);
    }

    @Override
    public String toString() {
        return type + ":" + identifier.getFullName();
    }
}

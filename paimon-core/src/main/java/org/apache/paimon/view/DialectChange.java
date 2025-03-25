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

package org.apache.paimon.view;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

// Dialect change to view.
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = DialectChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DialectChange.AddDialect.class, name = DialectChange.Actions.ADD)
})
public interface DialectChange extends Serializable {

    static DialectChange add(String dialect, String query, boolean force) {
        return new AddDialect(dialect, query, force);
    }

    /** add dialect for dialect change. */
    final class AddDialect implements DialectChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";
        private static final String FIELD_FORCE = "force";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonProperty(FIELD_FORCE)
        private final boolean force;

        @JsonCreator
        public AddDialect(
                @JsonProperty(FIELD_DIALECT) String dialect,
                @JsonProperty(FIELD_QUERY) String query,
                @JsonProperty(FIELD_FORCE) boolean force) {
            this.dialect = dialect;
            this.query = query;
            this.force = force;
        }

        @JsonGetter(FIELD_DIALECT)
        public String getDialect() {
            return dialect;
        }

        @JsonGetter(FIELD_QUERY)
        public String getQuery() {
            return query;
        }

        @JsonGetter(FIELD_FORCE)
        public boolean isForce() {
            return force;
        }
    }

    /** Actions for view alter. */
    class Actions {
        public static final String FIELD_TYPE = "action";
        public static final String ADD = "add";

        private Actions() {}
    }
}

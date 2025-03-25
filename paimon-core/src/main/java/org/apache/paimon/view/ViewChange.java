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

/** Dialect change to view. */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = ViewChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ViewChange.AddView.class, name = ViewChange.Actions.ADD),
    @JsonSubTypes.Type(value = ViewChange.UpdateView.class, name = ViewChange.Actions.UPDATE),
    @JsonSubTypes.Type(value = ViewChange.DropView.class, name = ViewChange.Actions.DROP)
})
public interface ViewChange extends Serializable {

    static ViewChange add(String dialect, String query) {
        return new AddView(dialect, query);
    }

    static ViewChange update(String dialect, String query) {
        return new UpdateView(dialect, query);
    }

    static ViewChange drop(String dialect) {
        return new DropView(dialect);
    }

    /** add dialect for dialect change. */
    final class AddView implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonCreator
        public AddView(
                @JsonProperty(FIELD_DIALECT) String dialect,
                @JsonProperty(FIELD_QUERY) String query) {
            this.dialect = dialect;
            this.query = query;
        }

        @JsonGetter(FIELD_DIALECT)
        public String getDialect() {
            return dialect;
        }

        @JsonGetter(FIELD_QUERY)
        public String getQuery() {
            return query;
        }
    }

    /** update dialect for dialect change. */
    final class UpdateView implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonCreator
        public UpdateView(
                @JsonProperty(FIELD_DIALECT) String dialect,
                @JsonProperty(FIELD_QUERY) String query) {
            this.dialect = dialect;
            this.query = query;
        }

        @JsonGetter(FIELD_DIALECT)
        public String getDialect() {
            return dialect;
        }

        @JsonGetter(FIELD_QUERY)
        public String getQuery() {
            return query;
        }
    }

    final class DropView implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonCreator
        public DropView(@JsonProperty(FIELD_DIALECT) String dialect) {
            this.dialect = dialect;
        }

        @JsonGetter(FIELD_DIALECT)
        public String getDialect() {
            return dialect;
        }
    }

    /** Actions for view alter. */
    class Actions {
        public static final String FIELD_TYPE = "action";
        public static final String ADD = "add";
        public static final String UPDATE = "update";
        public static final String DROP = "drop";

        private Actions() {}
    }
}

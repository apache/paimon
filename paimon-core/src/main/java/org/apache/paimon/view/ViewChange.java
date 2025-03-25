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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Dialect change to view. */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = ViewChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = ViewChange.SetOption.class,
            name = ViewChange.Actions.SET_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.RemoveOption.class,
            name = ViewChange.Actions.REMOVE_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.UpdateComment.class,
            name = ViewChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.AddDialect.class,
            name = ViewChange.Actions.ADD_DIALECT_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.UpdateDialect.class,
            name = ViewChange.Actions.UPDATE_DIALECT_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.DropDialect.class,
            name = ViewChange.Actions.DROP_DIALECT_ACTION)
})
public interface ViewChange extends Serializable {

    static ViewChange setOption(String key, String value) {
        return new ViewChange.SetOption(key, value);
    }

    static ViewChange removeOption(String key) {
        return new ViewChange.RemoveOption(key);
    }

    static ViewChange updateComment(String comment) {
        return new ViewChange.UpdateComment(comment);
    }

    static ViewChange addDialect(String dialect, String query) {
        return new AddDialect(dialect, query);
    }

    static ViewChange updateDialect(String dialect, String query) {
        return new UpdateDialect(dialect, query);
    }

    static ViewChange dropDialect(String dialect) {
        return new DropDialect(dialect);
    }

    /** addDialect dialect for view change. */
    final class AddDialect implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonCreator
        public AddDialect(
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

    /** update dialect for view change. */
    final class UpdateDialect implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonCreator
        public UpdateDialect(
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

    /** drop dialect for view change. */
    final class DropDialect implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonCreator
        public DropDialect(@JsonProperty(FIELD_DIALECT) String dialect) {
            this.dialect = dialect;
        }

        @JsonGetter(FIELD_DIALECT)
        public String getDialect() {
            return dialect;
        }
    }

    /** set a view option for view change. */
    final class SetOption implements ViewChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        @JsonProperty(FIELD_KEY)
        private final String key;

        @JsonProperty(FIELD_VALUE)
        private final String value;

        @JsonCreator
        private SetOption(
                @JsonProperty(FIELD_KEY) String key, @JsonProperty(FIELD_VALUE) String value) {
            this.key = key;
            this.value = value;
        }

        @JsonGetter(FIELD_KEY)
        public String key() {
            return key;
        }

        @JsonGetter(FIELD_VALUE)
        public String value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SetOption that = (SetOption) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /** remove a view option for view change. */
    final class RemoveOption implements ViewChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";

        @JsonProperty(FIELD_KEY)
        private final String key;

        private RemoveOption(@JsonProperty(FIELD_KEY) String key) {
            this.key = key;
        }

        @JsonGetter(FIELD_KEY)
        public String key() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RemoveOption that = (RemoveOption) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    /** update a view comment for view change. */
    final class UpdateComment implements ViewChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_COMMENT = "comment";

        // If comment is null, means to remove comment
        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        private UpdateComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
            this.comment = comment;
        }

        @JsonGetter(FIELD_COMMENT)
        public @Nullable String comment() {
            return comment;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateComment that = (UpdateComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /** Actions for view alter. */
    class Actions {
        public static final String FIELD_TYPE = "action";
        public static final String ADD_DIALECT_ACTION = "addDialect";
        public static final String UPDATE_DIALECT_ACTION = "updateDialect";
        public static final String DROP_DIALECT_ACTION = "dropDialect";
        public static final String SET_OPTION_ACTION = "setOption";
        public static final String REMOVE_OPTION_ACTION = "removeOption";
        public static final String UPDATE_COMMENT_ACTION = "updateComment";

        private Actions() {}
    }
}

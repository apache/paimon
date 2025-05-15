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

package org.apache.paimon.function;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Function change. */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = FunctionChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = FunctionChange.SetFunctionOption.class,
            name = FunctionChange.Actions.SET_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.RemoveFunctionOption.class,
            name = FunctionChange.Actions.REMOVE_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.UpdateFunctionComment.class,
            name = FunctionChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.AddDefinition.class,
            name = FunctionChange.Actions.ADD_DEFINITION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.UpdateDefinition.class,
            name = FunctionChange.Actions.UPDATE_DEFINITION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.DropDefinition.class,
            name = FunctionChange.Actions.DROP_DEFINITION_ACTION)
})
public interface FunctionChange extends Serializable {

    static FunctionChange setOption(String key, String value) {
        return new FunctionChange.SetFunctionOption(key, value);
    }

    static FunctionChange removeOption(String key) {
        return new FunctionChange.RemoveFunctionOption(key);
    }

    static FunctionChange updateComment(String comment) {
        return new FunctionChange.UpdateFunctionComment(comment);
    }

    static FunctionChange addDefinition(String name, FunctionDefinition definition) {
        return new FunctionChange.AddDefinition(name, definition);
    }

    static FunctionChange updateDefinition(String name, FunctionDefinition definition) {
        return new FunctionChange.UpdateDefinition(name, definition);
    }

    static FunctionChange dropDefinition(String name) {
        return new FunctionChange.DropDefinition(name);
    }

    /** set a function option for function change. */
    final class SetFunctionOption implements FunctionChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        @JsonProperty(FIELD_KEY)
        private final String key;

        @JsonProperty(FIELD_VALUE)
        private final String value;

        @JsonCreator
        private SetFunctionOption(
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
            SetFunctionOption that = (SetFunctionOption) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /** remove a function option for function change. */
    final class RemoveFunctionOption implements FunctionChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";

        @JsonProperty(FIELD_KEY)
        private final String key;

        private RemoveFunctionOption(@JsonProperty(FIELD_KEY) String key) {
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
            RemoveFunctionOption that = (RemoveFunctionOption) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    /** update a function comment for function change. */
    final class UpdateFunctionComment implements FunctionChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_COMMENT = "comment";

        // If comment is null, means to remove comment
        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        private UpdateFunctionComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
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
            UpdateFunctionComment that = (UpdateFunctionComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /** add definition for function change. */
    final class AddDefinition implements FunctionChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DEFINITION_NAME = "name";
        private static final String FIELD_DEFINITION = "definition";

        @JsonProperty(FIELD_DEFINITION_NAME)
        private final String name;

        @JsonProperty(FIELD_DEFINITION)
        private final FunctionDefinition definition;

        @JsonCreator
        public AddDefinition(
                @JsonProperty(FIELD_DEFINITION_NAME) String name,
                @JsonProperty(FIELD_DEFINITION) FunctionDefinition definition) {
            this.name = name;
            this.definition = definition;
        }

        @JsonGetter(FIELD_DEFINITION_NAME)
        public String name() {
            return name;
        }

        @JsonGetter(FIELD_DEFINITION)
        public FunctionDefinition definition() {
            return definition;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            AddDefinition that = (AddDefinition) object;
            return Objects.equals(name, that.name) && Objects.equals(definition, that.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, definition);
        }
    }

    /** update definition for function change. */
    final class UpdateDefinition implements FunctionChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DEFINITION_NAME = "name";
        private static final String FIELD_DEFINITION = "definition";

        @JsonProperty(FIELD_DEFINITION_NAME)
        private final String name;

        @JsonProperty(FIELD_DEFINITION)
        private final FunctionDefinition definition;

        @JsonCreator
        public UpdateDefinition(
                @JsonProperty(FIELD_DEFINITION_NAME) String name,
                @JsonProperty(FIELD_DEFINITION) FunctionDefinition definition) {
            this.name = name;
            this.definition = definition;
        }

        @JsonGetter(FIELD_DEFINITION_NAME)
        public String name() {
            return name;
        }

        @JsonGetter(FIELD_DEFINITION)
        public FunctionDefinition definition() {
            return definition;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateDefinition that = (UpdateDefinition) object;
            return Objects.equals(name, that.name) && Objects.equals(definition, that.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(definition, definition);
        }
    }

    /** drop definition for function change. */
    final class DropDefinition implements FunctionChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DEFINITION_NAME = "name";

        @JsonProperty(FIELD_DEFINITION_NAME)
        private final String name;

        @JsonCreator
        public DropDefinition(@JsonProperty(FIELD_DEFINITION_NAME) String name) {
            this.name = name;
        }

        @JsonGetter(FIELD_DEFINITION_NAME)
        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            DropDefinition that = (DropDefinition) object;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    /** Actions for function alter. */
    class Actions {
        public static final String FIELD_TYPE = "action";
        public static final String ADD_DEFINITION_ACTION = "addDefinition";
        public static final String UPDATE_DEFINITION_ACTION = "updateDefinition";
        public static final String DROP_DEFINITION_ACTION = "dropDefinition";
        public static final String SET_OPTION_ACTION = "setOption";
        public static final String REMOVE_OPTION_ACTION = "removeOption";
        public static final String UPDATE_COMMENT_ACTION = "updateComment";

        private Actions() {}
    }
}

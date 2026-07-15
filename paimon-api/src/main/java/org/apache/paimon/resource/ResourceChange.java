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

package org.apache.paimon.resource;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Resource change. */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = ResourceChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = ResourceChange.UpdateResourceComment.class,
            name = ResourceChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = ResourceChange.UpdateResourceUri.class,
            name = ResourceChange.Actions.UPDATE_URI_ACTION)
})
public interface ResourceChange extends Serializable {

    static ResourceChange updateComment(@Nullable String comment) {
        return new UpdateResourceComment(comment);
    }

    static ResourceChange updateUri(String uri) {
        return new UpdateResourceUri(uri);
    }

    /** Update comment for resource change. */
    final class UpdateResourceComment implements ResourceChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_COMMENT = "comment";

        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        @JsonCreator
        private UpdateResourceComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
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
            UpdateResourceComment that = (UpdateResourceComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /** Update URI for resource change. */
    final class UpdateResourceUri implements ResourceChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_URI = "uri";

        @JsonProperty(FIELD_URI)
        private final String uri;

        @JsonCreator
        private UpdateResourceUri(@JsonProperty(FIELD_URI) String uri) {
            this.uri = uri;
        }

        @JsonGetter(FIELD_URI)
        public String uri() {
            return uri;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateResourceUri that = (UpdateResourceUri) object;
            return Objects.equals(uri, that.uri);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uri);
        }
    }

    /** Actions for resource change. */
    class Actions {
        static final String FIELD_TYPE = "action";
        static final String UPDATE_COMMENT_ACTION = "updateComment";
        static final String UPDATE_URI_ACTION = "updateUri";

        private Actions() {}
    }
}

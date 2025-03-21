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

package org.apache.paimon.table;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/** table rollback instant. */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = Instant.Types.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(value = Instant.SnapshotInstant.class, name = Instant.Types.SNAPSHOT),
    @JsonSubTypes.Type(value = Instant.TagInstant.class, name = Instant.Types.TAG)
})
public interface Instant extends Serializable {

    static Instant snapshot(Long snapshotId) {
        return new SnapshotInstant(snapshotId);
    }

    static Instant tag(String tagName) {
        return new TagInstant(tagName);
    }

    /** snapshot instant for table rollback. */
    final class SnapshotInstant implements Instant {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_SNAPSHOT_ID = "snapshotId";

        @JsonProperty(FIELD_SNAPSHOT_ID)
        private final long snapshotId;

        @JsonCreator
        public SnapshotInstant(@JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId) {
            this.snapshotId = snapshotId;
        }

        @JsonGetter(FIELD_SNAPSHOT_ID)
        public long getSnapshotId() {
            return snapshotId;
        }
    }

    /** tag instant for table rollback. */
    final class TagInstant implements Instant {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_TAG_NAME = "tagName";

        @JsonProperty(FIELD_TAG_NAME)
        private final String tagName;

        @JsonCreator
        public TagInstant(@JsonProperty(FIELD_TAG_NAME) String tagName) {
            this.tagName = tagName;
        }

        @JsonGetter(FIELD_TAG_NAME)
        public String getTagName() {
            return tagName;
        }
    }

    /** Types for table rollback： identify for table rollback. */
    class Types {
        public static final String FIELD_TYPE = "type";
        public static final String SNAPSHOT = "snapshot";
        public static final String TAG = "tag";

        private Types() {}
    }
}

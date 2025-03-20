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

package org.apache.paimon.rest.requests;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/** table rollback instance. */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = TableRollbackToInstant.Types.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = TableRollbackToInstant.RollbackSnapshot.class,
            name = TableRollbackToInstant.Types.SNAPSHOT),
    @JsonSubTypes.Type(
            value = TableRollbackToInstant.RollbackTag.class,
            name = TableRollbackToInstant.Types.TAG)
})
public interface TableRollbackToInstant extends Serializable {

    static TableRollbackToInstant snapshot(Long snapshotId) {
        return new RollbackSnapshot(snapshotId);
    }

    static TableRollbackToInstant tag(String tagName) {
        return new RollbackTag(tagName);
    }

    /** snapshot instance for table rollback. */
    final class RollbackSnapshot implements TableRollbackToInstant {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_SNAPSHOT_ID = "snapshotId";

        @JsonProperty(FIELD_SNAPSHOT_ID)
        private final long snapshotId;

        @JsonCreator
        public RollbackSnapshot(@JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId) {
            this.snapshotId = snapshotId;
        }

        @JsonGetter(FIELD_SNAPSHOT_ID)
        public long getSnapshotId() {
            return snapshotId;
        }
    }

    /** tag instance for table rollback. */
    final class RollbackTag implements TableRollbackToInstant {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_TAG_NAME = "tagName";

        @JsonProperty(FIELD_TAG_NAME)
        private final String tagName;

        @JsonCreator
        public RollbackTag(@JsonProperty(FIELD_TAG_NAME) String tagName) {
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

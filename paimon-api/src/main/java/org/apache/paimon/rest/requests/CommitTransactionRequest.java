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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Request for committing snapshots of multiple tables atomically. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitTransactionRequest implements RESTRequest {

    private static final String FIELD_TABLE_CHANGES = "tableChanges";

    @JsonProperty(FIELD_TABLE_CHANGES)
    private final List<TableChange> tableChanges;

    @JsonCreator
    public CommitTransactionRequest(
            @JsonProperty(FIELD_TABLE_CHANGES) List<TableChange> tableChanges) {
        this.tableChanges = tableChanges;
    }

    @JsonGetter(FIELD_TABLE_CHANGES)
    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    /** A retry after an ambiguous response may turn a committed transaction into a conflict. */
    @JsonIgnore
    @Override
    public boolean isRetrySafe() {
        return false;
    }

    /** A table identifier and its snapshot commit request. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TableChange {

        private static final String FIELD_IDENTIFIER = "identifier";
        private static final String FIELD_COMMIT = "commit";

        @JsonProperty(FIELD_IDENTIFIER)
        private final Identifier identifier;

        @JsonProperty(FIELD_COMMIT)
        private final CommitTableRequest commit;

        @JsonCreator
        public TableChange(
                @JsonProperty(FIELD_IDENTIFIER) Identifier identifier,
                @JsonProperty(FIELD_COMMIT) CommitTableRequest commit) {
            this.identifier = identifier;
            this.commit = commit;
        }

        @JsonGetter(FIELD_IDENTIFIER)
        public Identifier getIdentifier() {
            return identifier;
        }

        @JsonGetter(FIELD_COMMIT)
        public CommitTableRequest getCommit() {
            return commit;
        }
    }
}

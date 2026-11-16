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

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lifecycle and optional termination details of an asynchronous prediction. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PredictionStatus {

    /** Current lifecycle state. */
    public enum State {
        PENDING,
        RUNNING,
        TERMINATING,
        TERMINATED
    }

    /** Result of a terminated prediction. */
    public enum TerminationCode {
        SUCCESS,
        SUCCESS_WITH_FAILURES,
        FAILED,
        CANCELED
    }

    /** Result and diagnostic message available after termination. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TerminationDetails {

        private final TerminationCode code;
        private final @Nullable String message;

        @JsonCreator
        public TerminationDetails(
                @JsonProperty("code") TerminationCode code,
                @Nullable @JsonProperty("message") String message) {
            checkArgument(code != null, "code must not be null");
            this.code = code;
            this.message = message;
        }

        @JsonGetter("code")
        public TerminationCode code() {
            return code;
        }

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonGetter("message")
        public String message() {
            return message;
        }
    }

    private final State state;
    private final @Nullable TerminationDetails terminationDetails;

    @JsonCreator
    public PredictionStatus(
            @JsonProperty("state") State state,
            @Nullable @JsonProperty("terminationDetails") TerminationDetails terminationDetails) {
        checkArgument(state != null, "state must not be null");
        boolean terminating = state == State.TERMINATING || state == State.TERMINATED;
        checkArgument(
                terminating == (terminationDetails != null),
                "terminationDetails must be present only for terminating states");
        this.state = state;
        this.terminationDetails = terminationDetails;
    }

    @JsonGetter("state")
    public State state() {
        return state;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("terminationDetails")
    public TerminationDetails terminationDetails() {
        return terminationDetails;
    }

    public boolean isTerminal() {
        return state == State.TERMINATED;
    }
}

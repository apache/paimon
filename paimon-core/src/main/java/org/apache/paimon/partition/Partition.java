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

package org.apache.paimon.partition;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/** Represent a partition, including statistics and done flag. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class Partition extends PartitionStatistics {

    private static final long serialVersionUID = 2L;

    public static final String FIELD_DONE = "done";

    @JsonProperty(FIELD_DONE)
    private final boolean done;

    @JsonCreator
    public Partition(
            @JsonProperty(FIELD_SPEC) Map<String, String> spec,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime,
            @JsonProperty(FIELD_DONE) boolean done) {
        super(spec, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
        this.done = done;
    }

    @JsonGetter(FIELD_DONE)
    public boolean done() {
        return done;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Partition partition = (Partition) o;
        return done == partition.done;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), done);
    }

    @Override
    public String toString() {
        return "{"
                + "spec="
                + spec
                + ", recordCount="
                + recordCount
                + ", fileSizeInBytes="
                + fileSizeInBytes
                + ", fileCount="
                + fileCount
                + ", lastFileCreationTime="
                + lastFileCreationTime
                + ", done="
                + done
                + '}';
    }
}

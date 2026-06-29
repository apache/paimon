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

package org.apache.paimon.stats;

import org.apache.paimon.annotation.Experimental;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Statistics blob payload and the metadata needed to describe it after writing. */
@Experimental
public class StatisticsBlob {

    private final String type;
    private final List<Integer> fieldIds;
    @Nullable private final Long snapshotId;
    @Nullable private final Long sequenceNumber;
    private final Map<String, String> properties;
    private final byte[] payload;

    public StatisticsBlob(
            String type,
            @Nullable List<Integer> fieldIds,
            @Nullable Long snapshotId,
            @Nullable Long sequenceNumber,
            @Nullable Map<String, String> properties,
            byte[] payload) {
        this.type = checkNotNull(type, "type must not be null");
        this.fieldIds =
                fieldIds == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(new ArrayList<>(fieldIds));
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.properties =
                properties == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.payload =
                Arrays.copyOf(checkNotNull(payload, "payload must not be null"), payload.length);
    }

    public String type() {
        return type;
    }

    public List<Integer> fieldIds() {
        return fieldIds;
    }

    @Nullable
    public Long snapshotId() {
        return snapshotId;
    }

    @Nullable
    public Long sequenceNumber() {
        return sequenceNumber;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    StatisticsBlobMetadata toMetadata(String fileLocation, long offset, long length) {
        return new StatisticsBlobMetadata(
                type,
                fieldIds,
                snapshotId,
                sequenceNumber,
                properties,
                fileLocation,
                offset,
                length);
    }
}

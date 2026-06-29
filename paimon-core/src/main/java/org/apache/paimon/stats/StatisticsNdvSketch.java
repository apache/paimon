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
import org.apache.paimon.utils.ThetaSketch;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utilities for NDV statistics stored as Apache DataSketches Theta sketches. */
@Experimental
public class StatisticsNdvSketch {

    /**
     * Paimon-specific blob type. The payload is an Apache DataSketches compact Theta sketch, but
     * the sidecar container is Paimon's own statistics sidecar format, not an Iceberg Puffin file.
     */
    public static final String THETA_SKETCH_BLOB_TYPE = "paimon-ndv-theta-sketch-v1";

    public static final String PROPERTY_NDV = "ndv";

    private StatisticsNdvSketch() {}

    public static StatisticsBlob toBlob(
            int fieldId,
            @Nullable Long snapshotId,
            @Nullable Long sequenceNumber,
            byte[] thetaSketchBytes) {
        return toBlob(
                Collections.singletonList(fieldId), snapshotId, sequenceNumber, thetaSketchBytes);
    }

    public static StatisticsBlob toBlob(
            List<Integer> fieldIds,
            @Nullable Long snapshotId,
            @Nullable Long sequenceNumber,
            byte[] thetaSketchBytes) {
        Map<String, String> properties = new LinkedHashMap<>();
        // Cache the estimate in metadata so readers do not need to open the payload for NDV.
        properties.put(PROPERTY_NDV, Double.toString(estimate(thetaSketchBytes)));
        return new StatisticsBlob(
                THETA_SKETCH_BLOB_TYPE,
                fieldIds,
                snapshotId,
                sequenceNumber,
                properties,
                thetaSketchBytes);
    }

    public static byte[] union(byte[] sketchBytes1, byte[] sketchBytes2) {
        return ThetaSketch.union(sketchBytes1, sketchBytes2);
    }

    public static double estimate(byte[] thetaSketchBytes) {
        return ThetaSketch.estimate(thetaSketchBytes);
    }

    public static double ndv(StatisticsBlobMetadata metadata) {
        checkArgument(
                THETA_SKETCH_BLOB_TYPE.equals(metadata.type()),
                "Unsupported NDV sketch blob type %s.",
                metadata.type());
        String ndv = metadata.properties().get(PROPERTY_NDV);
        checkArgument(
                ndv != null, "NDV sketch blob metadata must contain property '%s'.", PROPERTY_NDV);
        return Double.parseDouble(ndv);
    }
}

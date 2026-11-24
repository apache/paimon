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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.util.Objects;

/** A router, vector shard or scalar bitmap file in a centroid index. */
public final class CentroidIndexFile extends GlobalIndexIOMeta {

    private final boolean routingModel;
    private final boolean scalarIndex;
    private final int countOrId;

    public CentroidIndexFile(
            Path path,
            long size,
            long rows,
            @Nullable byte[] payload,
            boolean routingModel,
            boolean scalarIndex,
            int countOrId) {
        super(path, size, rows, payload);
        this.routingModel = routingModel;
        this.scalarIndex = scalarIndex;
        this.countOrId = countOrId;
    }

    public boolean routingModel() {
        return routingModel;
    }

    public boolean scalarIndex() {
        return scalarIndex;
    }

    /** Number of centroids in this router. */
    public int nlist() {
        if (!routingModel) {
            throw new IllegalStateException("Expected a routing model.");
        }
        return countOrId;
    }

    /** Centroid to which this shard belongs. */
    public int centroid() {
        if (routingModel || scalarIndex) {
            throw new IllegalStateException("Expected a centroid shard.");
        }
        return countOrId;
    }

    /** Table field indexed by this Bitmap. */
    public int fieldId() {
        if (!scalarIndex) {
            throw new IllegalStateException("Expected a centroid bitmap.");
        }
        return countOrId;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        CentroidIndexFile file = (CentroidIndexFile) other;
        return routingModel == file.routingModel
                && scalarIndex == file.scalarIndex
                && countOrId == file.countOrId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), routingModel, scalarIndex, countOrId);
    }
}

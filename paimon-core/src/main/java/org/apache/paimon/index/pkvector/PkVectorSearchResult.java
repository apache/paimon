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

package org.apache.paimon.index.pkvector;

/** One vector search result addressed by source data-file row position. */
public final class PkVectorSearchResult {

    private final String dataFileName;
    private final long rowPosition;
    private final float distance;

    public PkVectorSearchResult(String dataFileName, long rowPosition, float distance) {
        this.dataFileName = dataFileName;
        this.rowPosition = rowPosition;
        this.distance = distance;
    }

    public String dataFileName() {
        return dataFileName;
    }

    public long rowPosition() {
        return rowPosition;
    }

    public float distance() {
        return distance;
    }
}

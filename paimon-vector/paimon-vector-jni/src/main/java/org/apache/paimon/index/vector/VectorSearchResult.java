// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.paimon.index.vector;

import java.util.Arrays;

public final class VectorSearchResult {

    private final long[] ids;
    private final float[] distances;

    public VectorSearchResult(long[] ids, float[] distances) {
        if (ids == null) {
            throw new NullPointerException("ids");
        }
        if (distances == null) {
            throw new NullPointerException("distances");
        }
        if (ids.length != distances.length) {
            throw new IllegalArgumentException(
                    "ids length " + ids.length + " != distances length " + distances.length);
        }
        this.ids = ids.clone();
        this.distances = distances.clone();
    }

    public int size() {
        return ids.length;
    }

    public long[] ids() {
        return ids.clone();
    }

    public float[] distances() {
        return distances.clone();
    }

    @Override
    public String toString() {
        return "VectorSearchResult{ids="
                + Arrays.toString(ids)
                + ", distances="
                + Arrays.toString(distances)
                + '}';
    }
}

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

package org.apache.paimon.data;

import org.apache.paimon.utils.UriReader;

/** Logical reader APIs used to obtain a BLOB payload, independent of physical storage location. */
public enum BlobTransferSource {
    INLINE("inline"),
    HTTP("http"),
    FILE("file"),
    CUSTOM("custom"),
    UNKNOWN("unknown");

    private final String metricValue;

    BlobTransferSource(String metricValue) {
        this.metricValue = metricValue;
    }

    public String metricValue() {
        return metricValue;
    }

    /**
     * Classifies standard readers without parsing, retaining or exposing a URI. Exact-class checks
     * avoid inspecting metadata of subclasses that may transform or replace the actual byte source.
     */
    public static BlobTransferSource classify(Blob blob) {
        if (blob != null && blob.getClass() == BlobData.class) {
            return INLINE;
        }
        if (blob == null || blob.getClass() != BlobRef.class) {
            return UNKNOWN;
        }
        UriReader reader = ((BlobRef) blob).uriReader();
        if (reader == null) {
            return UNKNOWN;
        }
        if (reader.getClass() == UriReader.HttpUriReader.class) {
            return HTTP;
        }
        if (reader.getClass() == UriReader.FileUriReader.class) {
            return FILE;
        }
        return CUSTOM;
    }
}

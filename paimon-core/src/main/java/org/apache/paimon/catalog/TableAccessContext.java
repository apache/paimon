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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Context describing why a table is accessed. */
public class TableAccessContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Type of table access. */
    public enum AccessType {
        DIRECT,
        BLOB_VIEW_FALLBACK
    }

    private final AccessType accessType;
    @Nullable private final Identifier fallbackFrom;

    private TableAccessContext(AccessType accessType, @Nullable Identifier fallbackFrom) {
        this.accessType = Objects.requireNonNull(accessType, "accessType");
        this.fallbackFrom = fallbackFrom;
    }

    public static TableAccessContext direct() {
        return new TableAccessContext(AccessType.DIRECT, null);
    }

    public static TableAccessContext blobViewFallback(Identifier fallbackFrom) {
        return new TableAccessContext(
                AccessType.BLOB_VIEW_FALLBACK,
                Objects.requireNonNull(fallbackFrom, "fallbackFrom"));
    }

    public AccessType accessType() {
        return accessType;
    }

    @Nullable
    public Identifier fallbackFrom() {
        return fallbackFrom;
    }

    public boolean isBlobViewFallback() {
        return accessType == AccessType.BLOB_VIEW_FALLBACK;
    }
}

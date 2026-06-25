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

package org.apache.paimon.io;

import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/**
 * Per-file physical write plan for row data files.
 *
 * <p>The physical row may use a different nested layout from the logical input row, but it must
 * keep the same top-level field names and arity as the logical write schema. File-level statistics,
 * index writers, and commit metadata still use logical top-level fields.
 */
public interface RowDataFileWritePlan {

    /** Returns the physical row type written to the file. */
    RowType physicalType();

    /**
     * Returns a transform from logical input rows to physical file rows, or {@code null} if rows
     * can be written directly.
     */
    @Nullable
    RowDataTransform rowTransform();

    /**
     * Returns a metadata finalizer to add file metadata before the writer is closed, or {@code
     * null} if no extra metadata is needed.
     */
    @Nullable
    FileMetadataFinalizer metadataFinalizer();

    /** Creates a plan that writes rows directly with the given row type and no extra metadata. */
    static RowDataFileWritePlan direct(RowType rowType) {
        return new RowDataFileWritePlan() {
            @Override
            public RowType physicalType() {
                return rowType;
            }

            @Nullable
            @Override
            public RowDataTransform rowTransform() {
                return null;
            }

            @Nullable
            @Override
            public FileMetadataFinalizer metadataFinalizer() {
                return null;
            }
        };
    }
}

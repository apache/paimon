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

package org.apache.paimon.arrow;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.ProjectableBundleRecords;
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Batch records for vector schema root. */
public class ArrowBundleRecords implements ProjectableBundleRecords {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final RowType rowType;
    private final boolean caseSensitive;

    public ArrowBundleRecords(
            VectorSchemaRoot vectorSchemaRoot, RowType rowType, boolean caseSensitive) {
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.rowType = rowType;
        this.caseSensitive = caseSensitive;
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }

    @Override
    public long rowCount() {
        return vectorSchemaRoot.getRowCount();
    }

    @Override
    public Iterator<InternalRow> iterator() {
        ArrowBatchReader arrowBatchReader = new ArrowBatchReader(rowType, caseSensitive);
        return arrowBatchReader.readBatch(vectorSchemaRoot).iterator();
    }

    @Override
    public ReplayableBundleRecords project(int[] projection) {
        if (isIdentityProjection(projection)) {
            return this;
        }

        return new ArrowBundleRecords(
                projectVectorSchemaRoot(vectorSchemaRoot, projection),
                rowType.project(projection),
                caseSensitive);
    }

    private boolean isIdentityProjection(int[] projection) {
        if (projection.length != rowType.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < projection.length; i++) {
            if (projection[i] != i) {
                return false;
            }
        }
        return true;
    }

    private static VectorSchemaRoot projectVectorSchemaRoot(
            VectorSchemaRoot vectorSchemaRoot, int[] projection) {
        List<Field> fields = new ArrayList<>(projection.length);
        List<FieldVector> vectors = new ArrayList<>(projection.length);
        for (int index : projection) {
            FieldVector vector = vectorSchemaRoot.getVector(index);
            fields.add(vector.getField());
            vectors.add(vector);
        }
        return new VectorSchemaRoot(fields, vectors, vectorSchemaRoot.getRowCount());
    }
}

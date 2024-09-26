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
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Iterator;

/** Batch records for vector schema root. */
public class ArrowBundleRecords implements BundleRecords {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final RowType rowType;
    private final boolean onlyLowerCase;

    public ArrowBundleRecords(
            VectorSchemaRoot vectorSchemaRoot, RowType rowType, boolean onlyLowerCase) {
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.rowType = rowType;
        this.onlyLowerCase = onlyLowerCase;
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
        ArrowBatchReader arrowBatchReader = new ArrowBatchReader(rowType, onlyLowerCase);
        return arrowBatchReader.readBatch(vectorSchemaRoot).iterator();
    }
}

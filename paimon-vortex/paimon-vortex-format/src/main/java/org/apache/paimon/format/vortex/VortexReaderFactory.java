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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import dev.vortex.api.Expression;

import javax.annotation.Nullable;

import java.util.List;

/** A factory to create Vortex Reader. */
public class VortexReaderFactory implements FormatReaderFactory {

    private final RowType dataSchemaRowType;
    private final RowType projectedRowType;
    @Nullable private final List<Predicate> predicates;
    private final int workerThreads;

    public VortexReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            int workerThreads) {
        this.dataSchemaRowType = dataSchemaRowType;
        this.projectedRowType = projectedRowType;
        this.predicates = predicates;
        this.workerThreads = workerThreads;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) {
        VortexSessions.ensureWorkerThreads(workerThreads);

        RoaringBitmap32 selection = context.selection();
        VortexInput input =
                VortexInput.of(context.fileIO(), context.filePath(), context.fileSize());

        // The scan clones the filter it is given, so this tree is ours to release once the
        // reader's constructor has built the scan.
        try (Expression filter = VortexPredicateConverter.toVortexExpression(predicates)) {
            return new VortexRecordsReader(
                    input,
                    context.filePath(),
                    dataSchemaRowType,
                    projectedRowType,
                    selection,
                    filter);
        }
    }
}

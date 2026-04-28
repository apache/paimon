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
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringBitmap32;

import dev.vortex.api.Expression;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.vortex.VortexUtils.toVortexSpecified;

/** A factory to create Vortex Reader. */
public class VortexReaderFactory implements FormatReaderFactory {

    private final RowType dataSchemaRowType;
    private final RowType projectedRowType;
    @Nullable private final List<Predicate> predicates;

    public VortexReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates) {
        this.dataSchemaRowType = dataSchemaRowType;
        this.projectedRowType = projectedRowType;
        this.predicates = predicates;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) {
        long[] rowIndices = toRowIndices(context.selection());
        Expression predicate = VortexPredicateConverter.toVortexExpression(predicates);
        Pair<Path, Map<String, String>> vortexSpecified =
                toVortexSpecified(context.fileIO(), context.filePath());
        return new VortexRecordsReader(
                vortexSpecified.getLeft(),
                dataSchemaRowType,
                projectedRowType,
                rowIndices,
                predicate,
                vortexSpecified.getRight());
    }

    @Nullable
    private static long[] toRowIndices(@Nullable RoaringBitmap32 selection) {
        if (selection == null) {
            return null;
        }
        long cardinality = selection.getCardinality();
        long[] indices = new long[(int) cardinality];
        Iterator<Integer> iter = selection.iterator();
        int i = 0;
        while (iter.hasNext()) {
            indices[i++] = iter.next();
        }
        return indices;
    }
}

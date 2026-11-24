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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Evaluates a scalar predicate across the Bitmap files of a centroid row-ID range. */
public final class CentroidBitmapIndex {

    private CentroidBitmapIndex() {}

    /** Positions for each centroid occupy a separate copy of the source row-ID range. */
    public static long encodeRowId(int centroid, long rowRangeSize, long relativeRowId) {
        return Math.addExact(Math.multiplyExact(centroid, rowRangeSize), relativeRowId);
    }

    public static IOFunction<Integer, RoaringNavigableMap64> filter(
            GlobalIndexFileReader files,
            Map<Integer, ? extends GlobalIndexIOMeta> bitmaps,
            RowType rowType,
            RowType buildType,
            Predicate predicate,
            long rowRangeSize,
            int nlist) {
        checkArgument(rowRangeSize > 0 && nlist > 0, "Invalid centroid bitmap range.");
        return new IOFunction<Integer, RoaringNavigableMap64>() {
            private Map<Integer, RoaringNavigableMap64> matches;

            @Override
            public synchronized RoaringNavigableMap64 apply(Integer centroid) throws IOException {
                if (matches == null) {
                    RoaringNavigableMap64 rows =
                            match(files, bitmaps, rowType, buildType, predicate);
                    matches = splitByCentroid(rows, rowRangeSize, nlist);
                }
                // The caller can intersect and serialize its bitmap independently.
                RoaringNavigableMap64 result = new RoaringNavigableMap64();
                RoaringNavigableMap64 rows = matches.get(centroid);
                if (rows != null) {
                    result.or(rows);
                }
                return result;
            }
        };
    }

    private static RoaringNavigableMap64 match(
            GlobalIndexFileReader files,
            Map<Integer, ? extends GlobalIndexIOMeta> bitmaps,
            RowType rowType,
            RowType buildType,
            Predicate predicate)
            throws IOException {
        Map<Integer, BitmapIndexReader> readers = new HashMap<>();
        try {
            return evaluate(
                    predicate,
                    fieldName -> {
                        int fieldId = rowType.getField(fieldName).id();
                        BitmapIndexReader reader = readers.get(fieldId);
                        if (reader == null) {
                            GlobalIndexIOMeta bitmap = bitmaps.get(fieldId);
                            checkState(
                                    bitmap != null,
                                    "Missing centroid bitmap for field %s.",
                                    fieldId);
                            KeySerializer serializer =
                                    KeySerializer.create(buildType.getField(fieldId).type());
                            reader = new BitmapIndexReader(serializer, files, bitmap);
                            readers.put(fieldId, reader);
                        }
                        return reader;
                    });
        } finally {
            IOUtils.closeAllQuietly(readers.values());
        }
    }

    private static Map<Integer, RoaringNavigableMap64> splitByCentroid(
            RoaringNavigableMap64 rows, long rowRangeSize, int nlist) {
        Map<Integer, RoaringNavigableMap64> matches = new HashMap<>();
        for (long row : rows) {
            long centroid = row / rowRangeSize;
            checkState(row >= 0 && centroid < nlist, "Invalid centroid bitmap position: %s.", row);
            matches.computeIfAbsent((int) centroid, ignored -> new RoaringNavigableMap64())
                    .add(row % rowRangeSize);
        }
        return matches;
    }

    private static RoaringNavigableMap64 evaluate(
            Predicate predicate, IOFunction<String, BitmapIndexReader> openReader)
            throws IOException {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            BitmapIndexReader reader = openReader.apply(leaf.fieldRefOptional().get().name());
            if (Equal.INSTANCE.equals(leaf.function())) {
                return reader.visitEqual(leaf.literals().get(0)).get().results();
            }
            if (IsNull.INSTANCE.equals(leaf.function())) {
                return reader.visitIsNull().get().results();
            }
            if (IsNotNull.INSTANCE.equals(leaf.function())) {
                return reader.visitIsNotNull().get().results();
            }
            checkArgument(
                    In.INSTANCE.equals(leaf.function()),
                    "Unsupported centroid bitmap predicate %s.",
                    leaf.function());
            return reader.visitIn(leaf.literals()).get().results();
        }

        checkArgument(
                predicate instanceof CompoundPredicate, "Unsupported centroid bitmap predicate.");
        CompoundPredicate compound = (CompoundPredicate) predicate;
        boolean conjunction = And.INSTANCE.equals(compound.function());
        checkArgument(
                conjunction || Or.INSTANCE.equals(compound.function()),
                "Unsupported centroid bitmap predicate %s.",
                compound.function());
        Iterator<Predicate> children = compound.children().iterator();
        checkArgument(children.hasNext(), "Centroid bitmap lookup requires a predicate.");
        RoaringNavigableMap64 rows = evaluate(children.next(), openReader);
        while (children.hasNext() && (!conjunction || !rows.isEmpty())) {
            RoaringNavigableMap64 childRows = evaluate(children.next(), openReader);
            if (conjunction) {
                rows.and(childRows);
            } else {
                rows.or(childRows);
            }
        }
        return rows;
    }
}

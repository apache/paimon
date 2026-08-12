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

import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;

/** Evaluate file index result. */
public class FileIndexEvaluator {

    // Roaring array containers hold at most 4096 values; larger results favor bulk operations.
    private static final int MAX_POINT_LOOKUPS = 4096;

    public static FileIndexResult evaluate(
            FileIO fileIO,
            TableSchema dataSchema,
            List<Predicate> dataFilter,
            @Nullable TopN topN,
            @Nullable Integer limit,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            @Nullable DeletionVector dv)
            throws IOException {
        if (isNullOrEmpty(dataFilter) && topN == null) {
            if (limit == null) {
                return FileIndexResult.REMAIN;
            } else {
                // limit can not work with other predicates.
                return createLimitSelection(file, dv, limit);
            }
        }

        if (isNullOrEmpty(dataFilter)
                && topN != null
                && (file.rowCount() > RoaringBitmap32.MAX_VALUE || !supportsBitmapSelection(dv))) {
            return FileIndexResult.REMAIN;
        }

        try (FileIndexPredicate predicate =
                createFileIndexPredicate(fileIO, dataSchema, dataFilePathFactory, file)) {
            if (predicate == null) {
                return FileIndexResult.REMAIN;
            }

            BitmapIndexResult selection = null;
            FileIndexResult result;
            if (!isNullOrEmpty(dataFilter)) {
                Predicate filter = PredicateBuilder.and(dataFilter.toArray(new Predicate[0]));
                result = predicate.evaluate(filter);
                if (result instanceof BitmapIndexResult) {
                    // Bitmap file indexes cannot represent positions beyond RoaringBitmap32.
                    if (file.rowCount() > RoaringBitmap32.MAX_VALUE) {
                        return FileIndexResult.REMAIN;
                    }
                    BitmapIndexResult bitmapResult = (BitmapIndexResult) result;
                    if (bitmapResult.get().getCardinality() == file.rowCount()) {
                        return FileIndexResult.REMAIN;
                    }
                    if (dv instanceof Bitmap64DeletionVector) {
                        result =
                                excludeDeletedPositions(
                                        bitmapResult, (Bitmap64DeletionVector) dv, file.rowCount());
                    } else if (supportsBitmapSelection(dv)) {
                        selection = createBaseSelection(file, dv);
                        result = result.and(selection);
                    }
                }
            } else if (topN != null) {
                // 1. TopN cannot work with filter, because a filter may not completely filter out
                // all records, any unfiltered records can affect the calculation results of TopN
                // 2. evaluateTopN with selection, because we must filter out the data based on
                // deletion vector before selecting TopN records.
                selection = createBaseSelection(file, dv);
                result = predicate.evaluateTopN(topN, selection);
            } else {
                return FileIndexResult.REMAIN;
            }

            // if all position selected, or if only and not the deletion
            // the effect will not obvious, just return REMAIN.
            if (selection != null && Objects.equals(result, selection)) {
                return FileIndexResult.REMAIN;
            }

            if (!result.remain()) {
                return FileIndexResult.SKIP;
            }

            return result;
        }
    }

    private static FileIndexResult createLimitSelection(
            DataFileMeta file, @Nullable DeletionVector dv, int limit) {
        if (dv == null) {
            return new BitmapIndexResult(
                    () -> RoaringBitmap32.bitmapOfRange(0, Math.min(file.rowCount(), limit)));
        }

        if (file.rowCount() > RoaringBitmap32.MAX_VALUE || !supportsBitmapSelection(dv)) {
            return FileIndexResult.REMAIN;
        }

        return createBaseSelection(file, dv).limit(limit);
    }

    private static BitmapIndexResult createBaseSelection(
            DataFileMeta file, @Nullable DeletionVector dv) {
        return new BitmapIndexResult(
                () -> {
                    RoaringBitmap32 selection = RoaringBitmap32.bitmapOfRange(0, file.rowCount());
                    if (dv == null) {
                        return selection;
                    }

                    RoaringBitmap32 deletion;
                    if (dv instanceof BitmapDeletionVector) {
                        deletion = ((BitmapDeletionVector) dv).get();
                    } else if (dv instanceof Bitmap64DeletionVector) {
                        deletion = ((Bitmap64DeletionVector) dv).projectToBitmap32(file.rowCount());
                    } else {
                        return selection;
                    }
                    selection.andNot(deletion);
                    return selection;
                });
    }

    private static boolean supportsBitmapSelection(@Nullable DeletionVector dv) {
        return dv == null
                || dv instanceof BitmapDeletionVector
                || dv instanceof Bitmap64DeletionVector;
    }

    private static BitmapIndexResult excludeDeletedPositions(
            BitmapIndexResult candidates, Bitmap64DeletionVector dv, long rowCount) {
        return new BitmapIndexResult(
                () -> {
                    RoaringBitmap32 candidateBitmap = candidates.get();
                    if (candidateBitmap.getCardinality() > MAX_POINT_LOOKUPS) {
                        return RoaringBitmap32.andNot(
                                candidateBitmap, dv.projectToBitmap32(rowCount));
                    }

                    RoaringBitmap32 result = new RoaringBitmap32();
                    Iterator<Integer> iterator = candidateBitmap.iterator();
                    while (iterator.hasNext()) {
                        int position = iterator.next();
                        if (!dv.isDeleted(position)) {
                            result.add(position);
                        }
                    }
                    return result;
                });
    }

    @Nullable
    private static FileIndexPredicate createFileIndexPredicate(
            FileIO fileIO,
            TableSchema dataSchema,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file)
            throws IOException {
        FileIndexPredicate predicate;
        byte[] embeddedIndex = file.embeddedIndex();
        if (embeddedIndex != null) {
            predicate = new FileIndexPredicate(embeddedIndex, dataSchema.logicalRowType());
        } else {
            List<String> indexFiles =
                    file.extraFiles().stream()
                            .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            if (indexFiles.isEmpty()) {
                return null;
            }
            if (indexFiles.size() > 1) {
                throw new RuntimeException(
                        "Found more than one index file for one data file: "
                                + String.join(" and ", indexFiles));
            }
            predicate =
                    new FileIndexPredicate(
                            dataFilePathFactory.toAlignedPath(indexFiles.get(0), file),
                            fileIO,
                            dataSchema.logicalRowType());
        }
        return predicate;
    }
}

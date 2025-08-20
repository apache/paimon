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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;

/** Evaluate file index result. */
public class FileIndexEvaluator {

    public static FileIndexResult evaluate(
            FileIO fileIO,
            TableSchema dataSchema,
            List<Predicate> dataFilter,
            @Nullable TopN topN,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            @Nullable DeletionVector deletionVector)
            throws IOException {
        if (isNullOrEmpty(dataFilter) && topN == null) {
            return FileIndexResult.REMAIN;
        }

        try (FileIndexPredicate predicate =
                createFileIndexPredicate(fileIO, dataSchema, dataFilePathFactory, file)) {
            if (predicate == null) {
                return FileIndexResult.REMAIN;
            }

            FileIndexResult selection =
                    new BitmapIndexResult(() -> RoaringBitmap32.bitmapOfRange(0, file.rowCount()));
            if (deletionVector instanceof BitmapDeletionVector) {
                RoaringBitmap32 deletion = ((BitmapDeletionVector) deletionVector).get();
                selection = ((BitmapIndexResult) selection).andNot(deletion);
            }
            FileIndexResult result;
            if (!isNullOrEmpty(dataFilter)) {
                Predicate filter = PredicateBuilder.and(dataFilter.toArray(new Predicate[0]));
                result = predicate.evaluate(filter);
                result.and(selection);
            } else if (topN != null) {
                result = predicate.evaluateTopN(topN, selection);
            } else {
                return FileIndexResult.REMAIN;
            }

            // if all position selected, or if only and not the deletion
            // the effect will not obvious, just return REMAIN.
            if (Objects.equals(result, selection)) {
                return FileIndexResult.REMAIN;
            }

            if (!result.remain()) {
                return FileIndexResult.SKIP;
            }

            return result;
        }
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

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

import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.TableSchema;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/** Evaluate file index result. */
public class FileIndexEvaluator {

    public static FileIndexResult evaluate(
            FileIO fileIO,
            TableSchema dataSchema,
            List<Predicate> dataFilter,
            TopN topN,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file)
            throws IOException {
        FileIndexResult result = FileIndexResult.REMAIN;
        if ((dataFilter == null || dataFilter.isEmpty()) && topN == null) {
            return result;
        }

        FileIndexPredicate predicate = null;
        try {
            byte[] embeddedIndex = file.embeddedIndex();
            if (embeddedIndex != null) {
                predicate = new FileIndexPredicate(embeddedIndex, dataSchema.logicalRowType());
            } else {
                List<String> indexFiles =
                        file.extraFiles().stream()
                                .filter(
                                        name ->
                                                name.endsWith(
                                                        DataFilePathFactory.INDEX_PATH_SUFFIX))
                                .collect(Collectors.toList());
                if (indexFiles.isEmpty()) {
                    return result;
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

            // data filter
            if (dataFilter != null && !dataFilter.isEmpty()) {
                result =
                        predicate.evaluate(
                                PredicateBuilder.and(dataFilter.toArray(new Predicate[0])));
            }

            // todo: We need to do more if the index filter supports pushdown.
            //  e.g. All pushdown index filters must exist and be evaluated.
            // topN filter
            if (dataFilter == null && topN != null) {
                result = predicate.evaluate(topN, result);
            }

            return result;
        } finally {
            if (predicate != null) {
                predicate.close();
            }
        }
    }
}

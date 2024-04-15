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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/** File index reader, do the filter in the constructor. */
public class FileIndexFileReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> reader;

    public FileIndexFileReader(
            FileIO fileIO,
            TableSchema dataSchema,
            List<Predicate> dataFilter,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            ConcatRecordReader.ReaderSupplier<InternalRow> readerSupplier)
            throws IOException {
        boolean filterThisFile = false;
        if (dataFilter != null && !dataFilter.isEmpty()) {
            List<String> indexFiles =
                    file.extraFiles().stream()
                            .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            if (!indexFiles.isEmpty()) {
                // go to file index check
                try (FileIndexPredicate predicate =
                        new FileIndexPredicate(
                                dataFilePathFactory.toPath(indexFiles.get(0)),
                                fileIO,
                                dataSchema.logicalRowType())) {
                    if (!predicate.testPredicate(
                            PredicateBuilder.and(dataFilter.toArray(new Predicate[0])))) {
                        filterThisFile = true;
                    }
                }
            }
        }

        this.reader = filterThisFile ? null : readerSupplier.get();
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        return reader == null ? null : reader.readBatch();
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}

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

package org.apache.paimon.fileindex;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils to check secondary index (e.g. bloom filter) predicate. */
public class FileIndexPredicate implements Closeable {

    private final FileIndexFormat.Reader reader;
    private final Map<String, FileIndexFieldPredicate> fieldPredicates = new HashMap<>();

    public FileIndexPredicate(Path path, FileIO fileIO, RowType fileRowType) throws IOException {
        this(fileIO.newInputStream(path), fileRowType);
    }

    public FileIndexPredicate(byte[] serializedBytes, RowType fileRowType) {
        this(new ByteArraySeekableStream(serializedBytes), fileRowType);
    }

    public FileIndexPredicate(SeekableInputStream inputStream, RowType fileRowType) {
        this.reader = FileIndexFormat.createReader(inputStream, fileRowType);
    }

    public boolean testPredicate(@Nullable Predicate filePredicate) {
        if (filePredicate == null) {
            return true;
        }

        Set<String> requredFieldNames = getRequiredNames(filePredicate);

        List<FileIndexFieldPredicate> testWorkers =
                requredFieldNames.stream()
                        .map(
                                cname ->
                                        fieldPredicates.computeIfAbsent(
                                                cname,
                                                k ->
                                                        new FileIndexFieldPredicate(
                                                                cname,
                                                                reader.readColumnIndex(cname))))
                        .collect(Collectors.toList());

        for (FileIndexFieldPredicate testWorker : testWorkers) {
            if (!testWorker.test(filePredicate)) {
                return false;
            }
        }
        return true;
    }

    private Set<String> getRequiredNames(Predicate filePredicate) {
        return filePredicate.visit(
                new PredicateVisitor<Set<String>>() {
                    final Set<String> names = new HashSet<>();

                    @Override
                    public Set<String> visit(LeafPredicate predicate) {
                        names.add(predicate.fieldName());
                        return names;
                    }

                    @Override
                    public Set<String> visit(CompoundPredicate predicate) {
                        for (Predicate child : predicate.children()) {
                            child.visit(this);
                        }
                        return names;
                    }
                });
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    /** Predicate test worker. */
    private static class FileIndexFieldPredicate implements PredicateVisitor<Boolean> {

        private final String columnName;
        private final List<FileIndexReader> fileIndexReaders;

        public FileIndexFieldPredicate(String columnName, List<FileIndexReader> fileIndexReaders) {
            this.columnName = columnName;
            this.fileIndexReaders = fileIndexReaders;
        }

        public Boolean test(Predicate predicate) {
            return predicate.visit(this);
        }

        @Override
        public Boolean visit(LeafPredicate predicate) {
            if (columnName.equals(predicate.fieldName())) {
                FieldRef fieldRef =
                        new FieldRef(predicate.index(), predicate.fieldName(), predicate.type());
                for (FileIndexReader fileIndexReader : fileIndexReaders) {
                    if (!predicate
                            .function()
                            .visit(fileIndexReader, fieldRef, predicate.literals())) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public Boolean visit(CompoundPredicate predicate) {

            if (predicate.function() instanceof Or) {
                for (Predicate predicate1 : predicate.children()) {
                    if (predicate1.visit(this)) {
                        return true;
                    }
                }
                return false;

            } else {
                for (Predicate predicate1 : predicate.children()) {
                    if (!predicate1.visit(this)) {
                        return false;
                    }
                }
                return true;
            }
        }
    }
}

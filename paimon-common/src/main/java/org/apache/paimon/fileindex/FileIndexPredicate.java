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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;

/** Utils to check secondary index (e.g. bloom filter) predicate. */
public class FileIndexPredicate implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileIndexPredicate.class);

    private final FileIndexFormat.Reader reader;

    @Nullable private Path path;

    public FileIndexPredicate(Path path, FileIO fileIO, RowType fileRowType) throws IOException {
        this(fileIO.newInputStream(path), fileRowType);
        this.path = path;
    }

    public FileIndexPredicate(byte[] serializedBytes, RowType fileRowType) {
        this(new ByteArraySeekableStream(serializedBytes), fileRowType);
    }

    public FileIndexPredicate(SeekableInputStream inputStream, RowType fileRowType) {
        this.reader = FileIndexFormat.createReader(inputStream, fileRowType);
    }

    public FileIndexResult evaluate(@Nullable Predicate predicate) {
        if (predicate == null) {
            return REMAIN;
        }
        Set<String> requiredFieldNames = getRequiredNames(predicate);
        Map<String, Collection<FileIndexReader>> indexReaders = new HashMap<>();
        requiredFieldNames.forEach(name -> indexReaders.put(name, reader.readColumnIndex(name)));
        return new FileIndexPredicateTest(indexReaders).test(predicate);
    }

    public boolean testPredicate(@Nullable Predicate filePredicate) {
        if (filePredicate == null) {
            return true;
        }

        Set<String> requiredFieldNames = getRequiredNames(filePredicate);

        Map<String, Collection<FileIndexReader>> indexReaders = new HashMap<>();
        requiredFieldNames.forEach(name -> indexReaders.put(name, reader.readColumnIndex(name)));
        if (!new FileIndexPredicateTest(indexReaders).test(filePredicate).remain()) {
            LOG.debug(
                    "One file has been filtered: "
                            + (path == null ? "in scan stage" : path.toString()));
            return false;
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
    private static class FileIndexPredicateTest implements PredicateVisitor<FileIndexResult> {

        private final Map<String, Collection<FileIndexReader>> columnIndexReaders;

        public FileIndexPredicateTest(Map<String, Collection<FileIndexReader>> fileIndexReaders) {
            this.columnIndexReaders = fileIndexReaders;
        }

        public FileIndexResult test(Predicate predicate) {
            return predicate.visit(this);
        }

        @Override
        public FileIndexResult visit(LeafPredicate predicate) {
            FileIndexResult compoundResult = REMAIN;
            FieldRef fieldRef =
                    new FieldRef(predicate.index(), predicate.fieldName(), predicate.type());
            for (FileIndexReader fileIndexReader : columnIndexReaders.get(predicate.fieldName())) {
                compoundResult =
                        compoundResult.and(
                                predicate
                                        .function()
                                        .visit(fileIndexReader, fieldRef, predicate.literals()));

                if (!compoundResult.remain()) {
                    return compoundResult;
                }
            }
            return compoundResult;
        }

        @Override
        public FileIndexResult visit(CompoundPredicate predicate) {
            if (predicate.function() instanceof Or) {
                FileIndexResult compoundResult = null;
                for (Predicate predicate1 : predicate.children()) {
                    compoundResult =
                            compoundResult == null
                                    ? predicate1.visit(this)
                                    : compoundResult.or(predicate1.visit(this));
                }
                return compoundResult == null ? REMAIN : compoundResult;
            } else {
                FileIndexResult compoundResult = null;
                for (Predicate predicate1 : predicate.children()) {
                    compoundResult =
                            compoundResult == null
                                    ? predicate1.visit(this)
                                    : compoundResult.and(predicate1.visit(this));
                    // if not remain, no need to test anymore
                    if (!compoundResult.remain()) {
                        return compoundResult;
                    }
                }
                return compoundResult == null ? REMAIN : compoundResult;
            }
        }
    }
}

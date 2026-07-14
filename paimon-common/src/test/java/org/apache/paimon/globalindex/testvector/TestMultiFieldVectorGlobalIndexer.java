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

package org.apache.paimon.globalindex.testvector;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Test-only vector indexer supporting one scalar extra field through a companion B-tree file. */
class TestMultiFieldVectorGlobalIndexer implements VectorGlobalIndexer {

    private final TestVectorGlobalIndexer vectorIndexer;
    private final BTreeGlobalIndexer scalarIndexer;
    private final DataField vectorField;
    private final DataField scalarField;

    TestMultiFieldVectorGlobalIndexer(
            DataField vectorField, List<DataField> extraFields, Options options) {
        checkArgument(
                extraFields.size() == 1,
                "Test multi-field vector index supports exactly one extra field, but got: %s",
                extraFields);
        this.vectorField = vectorField;
        this.scalarField = extraFields.get(0);
        this.vectorIndexer = new TestVectorGlobalIndexer(vectorField.type(), options);
        this.scalarIndexer = new BTreeGlobalIndexer(scalarField, options);
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        return new MultiColumnWriter(
                (GlobalIndexSingleColumnWriter) vectorIndexer.createWriter(fileWriter),
                (GlobalIndexSingleColumnWriter) scalarIndexer.createWriter(fileWriter),
                vectorField,
                scalarField);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            ExecutorService executor) {
        List<GlobalIndexIOMeta> vectorFiles = new ArrayList<>();
        List<GlobalIndexIOMeta> scalarFiles = new ArrayList<>();
        for (GlobalIndexIOMeta file : files) {
            // The test vector format has no file metadata; the B-tree companion always has it.
            (file.metadata() == null ? vectorFiles : scalarFiles).add(file);
        }
        checkArgument(vectorFiles.size() == 1, "Expected one test vector file, got: %s", files);
        checkArgument(
                scalarFiles.size() == 1, "Expected one scalar companion file, got: %s", files);
        return new MultiColumnReader(
                vectorIndexer.createReader(fileReader, vectorFiles, executor),
                scalarIndexer.createReader(fileReader, scalarFiles, executor));
    }

    @Override
    public String metric() {
        return vectorIndexer.metric();
    }

    private static class MultiColumnWriter implements GlobalIndexMultiColumnWriter {

        private final GlobalIndexSingleColumnWriter vectorWriter;
        private final GlobalIndexSingleColumnWriter scalarWriter;
        private final InternalRow.FieldGetter vectorGetter;
        private final InternalRow.FieldGetter scalarGetter;

        private MultiColumnWriter(
                GlobalIndexSingleColumnWriter vectorWriter,
                GlobalIndexSingleColumnWriter scalarWriter,
                DataField vectorField,
                DataField scalarField) {
            this.vectorWriter = vectorWriter;
            this.scalarWriter = scalarWriter;
            this.vectorGetter = InternalRow.createFieldGetter(vectorField.type(), 0);
            this.scalarGetter = InternalRow.createFieldGetter(scalarField.type(), 1);
        }

        @Override
        public void write(long rowId, @Nullable InternalRow row) {
            Object vector = row == null ? null : vectorGetter.getFieldOrNull(row);
            Object scalar = row == null ? null : scalarGetter.getFieldOrNull(row);
            vectorWriter.write(vector, rowId);
            scalarWriter.write(scalar, rowId);
        }

        @Override
        public List<ResultEntry> finish() {
            List<ResultEntry> results = new ArrayList<>();
            results.addAll(vectorWriter.finish());
            results.addAll(scalarWriter.finish());
            return results;
        }
    }

    /**
     * Routes vector operations to the vector file and scalar predicates to its B-tree companion.
     */
    private static class MultiColumnReader implements GlobalIndexReader {

        private final GlobalIndexReader vectorReader;
        private final GlobalIndexReader scalarReader;

        private MultiColumnReader(GlobalIndexReader vectorReader, GlobalIndexReader scalarReader) {
            this.vectorReader = vectorReader;
            this.scalarReader = scalarReader;
        }

        @Override
        public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
                VectorSearch vectorSearch) {
            return vectorReader.visitVectorSearch(vectorSearch);
        }

        @Override
        public CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> visitBatchVectorSearch(
                BatchVectorSearch batchVectorSearch) {
            return vectorReader.visitBatchVectorSearch(batchVectorSearch);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
            return scalarReader.visitIsNotNull(fieldRef);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
            return scalarReader.visitIsNull(fieldRef);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitStartsWith(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitEndsWith(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitContains(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitLike(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitLessThan(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitGreaterOrEqual(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitNotEqual(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitLessOrEqual(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitEqual(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
                FieldRef fieldRef, Object literal) {
            return scalarReader.visitGreaterThan(fieldRef, literal);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
                FieldRef fieldRef, List<Object> literals) {
            return scalarReader.visitIn(fieldRef, literals);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
                FieldRef fieldRef, List<Object> literals) {
            return scalarReader.visitNotIn(fieldRef, literals);
        }

        @Override
        public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
                FieldRef fieldRef, Object from, Object to) {
            return scalarReader.visitBetween(fieldRef, from, to);
        }

        @Override
        public void close() throws IOException {
            try {
                vectorReader.close();
            } finally {
                scalarReader.close();
            }
        }
    }
}

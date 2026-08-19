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
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** Exposes array-element membership over the bitmap global index format. */
public class MultiValueBitmapIndexReader implements GlobalIndexReader {

    private final KeySerializer keySerializer;
    private final LazyFilteredBitmapReader bitmapReader;

    MultiValueBitmapIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            long totalRowCount,
            ExecutorService executor) {
        this.keySerializer = keySerializer;
        this.bitmapReader =
                new LazyFilteredBitmapReader(
                        fileReader, files, keySerializer, 0, totalRowCount, executor);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitArrayContains(
            FieldRef fieldRef, Object literal) {
        return bitmapReader.visitEqual(fieldRef, literal);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitArraysOverlap(
            FieldRef fieldRef, List<Object> literals) {
        return bitmapReader.visitIn(fieldRef, literals);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitArrayContainsAll(
            FieldRef fieldRef, List<Object> literals) {
        if (literals.isEmpty()) {
            return unsupported();
        }

        Map<BitmapGlobalIndexFormat.SerializedKey, Object> distinctLiterals = new LinkedHashMap<>();
        for (Object literal : literals) {
            if (literal == null) {
                return CompletableFuture.completedFuture(
                        Optional.of(GlobalIndexResult.createEmpty()));
            }
            distinctLiterals.put(
                    BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, literal),
                    literal);
        }

        List<CompletableFuture<Optional<GlobalIndexResult>>> futures =
                new ArrayList<>(distinctLiterals.size());
        for (Object literal : distinctLiterals.values()) {
            futures.add(bitmapReader.visitEqual(fieldRef, literal));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            Optional<GlobalIndexResult> result = Optional.empty();
                            for (CompletableFuture<Optional<GlobalIndexResult>> future : futures) {
                                Optional<GlobalIndexResult> current = future.join();
                                if (!current.isPresent()) {
                                    return Optional.empty();
                                }
                                result =
                                        result.isPresent()
                                                ? Optional.of(result.get().and(current.get()))
                                                : current;
                            }
                            return result;
                        });
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotBetween(
            FieldRef fieldRef, Object from, Object to) {
        return unsupported();
    }

    @Override
    public void close() throws IOException {
        bitmapReader.close();
    }

    private static CompletableFuture<Optional<GlobalIndexResult>> unsupported() {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}

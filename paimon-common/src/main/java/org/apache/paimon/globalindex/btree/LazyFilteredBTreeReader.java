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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.UnionGlobalIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.predicate.FieldRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An Index Reader for BTree which dynamically filters file list by input predicate, then merge the
 * result by an {@link org.apache.paimon.globalindex.UnionGlobalIndexReader}. In the ideal situation
 * such as visiting an Equal predicate, only a very few files would be actually read.
 */
public class LazyFilteredBTreeReader implements GlobalIndexReader {

    private final BTreeFileMetaSelector fileSelector;
    private final List<GlobalIndexIOMeta> files;
    private final Map<String, GlobalIndexReader> readerCache;
    private final KeySerializer keySerializer;
    private final CacheManager cacheManager;
    private final GlobalIndexFileReader fileReader;

    public LazyFilteredBTreeReader(
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            CacheManager cacheManager) {
        this.fileSelector = new BTreeFileMetaSelector(files, keySerializer);
        this.readerCache = new HashMap<>();
        this.cacheManager = cacheManager;
        this.fileReader = fileReader;
        this.keySerializer = keySerializer;
        this.files = files;
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = fileSelector.visitIsNotNull(fieldRef);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitIsNotNull(fieldRef);
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = fileSelector.visitIsNull(fieldRef);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitIsNull(fieldRef);
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitStartsWith(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitStartsWith(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitEndsWith(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitEndsWith(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitContains(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitContains(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = fileSelector.visitLike(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitLike(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitLessThan(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitLessThan(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitGreaterOrEqual(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitGreaterOrEqual(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitNotEqual(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitNotEqual(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitLessOrEqual(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitLessOrEqual(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = fileSelector.visitEqual(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitEqual(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt =
                fileSelector.visitGreaterThan(fieldRef, literal);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitGreaterThan(fieldRef, literal);
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = fileSelector.visitIn(fieldRef, literals);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitIn(fieldRef, literals);
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = fileSelector.visitNotIn(fieldRef, literals);
        if (!selectedOpt.isPresent()) {
            return Optional.empty();
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        return createUnionReader(selected).visitNotIn(fieldRef, literals);
    }

    /**
     * Create a Union Reader for given files. The union reader is composed by readers from reader
     * cache, so please do not close it.
     */
    private UnionGlobalIndexReader createUnionReader(List<GlobalIndexIOMeta> files) {
        List<GlobalIndexReader> readers = new ArrayList<>();
        for (GlobalIndexIOMeta meta : files) {
            readers.add(
                    readerCache.computeIfAbsent(
                            meta.fileName(),
                            name -> {
                                try {
                                    return new BTreeIndexReader(
                                            keySerializer, fileReader, meta, cacheManager);
                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            "Can't create BTree index reader for " + name, e);
                                }
                            }));
        }
        return new UnionGlobalIndexReader(readers);
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (Map.Entry<String, GlobalIndexReader> entry : this.readerCache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException ioe) {
                if (exception == null) {
                    exception = ioe;
                } else {
                    exception.addSuppressed(ioe);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}

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
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedFileGlobalIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/** Reader for bitmap global index files. */
public class LazyFilteredBitmapReader extends SortedFileGlobalIndexReader<BitmapIndexReader> {

    private final GlobalIndexFileReader fileReader;
    private final KeySerializer keySerializer;

    LazyFilteredBitmapReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            long fallbackScanMaxSize,
            ExecutorService executor) {
        super(files, keySerializer, fallbackScanMaxSize, executor);
        this.fileReader = fileReader;
        this.keySerializer = keySerializer;
    }

    @Override
    protected Optional<GlobalIndexResult> visitIsNotNull(BitmapIndexReader reader) {
        return reader.visitIsNotNull();
    }

    @Override
    protected Optional<GlobalIndexResult> visitIsNull(BitmapIndexReader reader) {
        return reader.visitIsNull();
    }

    @Override
    protected Optional<GlobalIndexResult> visitStartsWith(
            BitmapIndexReader reader, Object literal) {
        return reader.visitStartsWith(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitEndsWith(BitmapIndexReader reader, Object literal) {
        return reader.visitEndsWith(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitContains(BitmapIndexReader reader, Object literal) {
        return reader.visitContains(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitLessThan(BitmapIndexReader reader, Object literal) {
        return reader.visitLessThan(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitGreaterOrEqual(
            BitmapIndexReader reader, Object literal) {
        return reader.visitGreaterOrEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitNotEqual(BitmapIndexReader reader, Object literal) {
        return reader.visitNotEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitLessOrEqual(
            BitmapIndexReader reader, Object literal) {
        return reader.visitLessOrEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitEqual(BitmapIndexReader reader, Object literal) {
        return reader.visitEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitGreaterThan(
            BitmapIndexReader reader, Object literal) {
        return reader.visitGreaterThan(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitIn(BitmapIndexReader reader, List<Object> literals) {
        return reader.visitIn(literals);
    }

    @Override
    protected Optional<GlobalIndexResult> visitNotIn(
            BitmapIndexReader reader, List<Object> literals) {
        return reader.visitNotIn(literals);
    }

    @Override
    protected Optional<GlobalIndexResult> visitBetween(
            BitmapIndexReader reader, Object from, Object to) {
        return reader.visitBetween(from, to);
    }

    @Override
    protected RoaringNavigableMap64 like(
            BitmapIndexReader reader, Function<Object, Boolean> keyPredicate) {
        return reader.like(keyPredicate::apply);
    }

    @Override
    protected RoaringNavigableMap64 lessThan(BitmapIndexReader reader, Object literal) {
        return reader.lessThan(literal);
    }

    @Override
    protected RoaringNavigableMap64 greaterThan(BitmapIndexReader reader, Object literal) {
        return reader.greaterThan(literal);
    }

    @Override
    protected BitmapIndexReader openReader(GlobalIndexIOMeta meta) {
        try {
            return new BitmapIndexReader(keySerializer, fileReader, meta);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Can't create bitmap index reader for " + meta.filePath(), e);
        }
    }
}

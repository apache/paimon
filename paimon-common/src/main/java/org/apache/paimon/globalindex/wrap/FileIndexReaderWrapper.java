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

package org.apache.paimon.globalindex.wrap;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.TopK;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/** A {@link GlobalIndexReader} wrapper for {@link FileIndexReader}. */
public class FileIndexReaderWrapper implements GlobalIndexReader {

    private final FileIndexReader reader;
    private final Function<FileIndexResult, GlobalIndexResult> transform;
    private final Closeable closeable;

    public FileIndexReaderWrapper(
            FileIndexReader reader,
            Function<FileIndexResult, GlobalIndexResult> transform,
            Closeable closeable) {
        this.reader = reader;
        this.transform = transform;
        this.closeable = closeable;
    }

    @Override
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return transform.apply(reader.visitIsNotNull(fieldRef));
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return transform.apply(reader.visitIsNull(fieldRef));
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitStartsWith(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitEndsWith(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitContains(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitLike(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitLessThan(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitNotEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return transform.apply(reader.visitIn(fieldRef, literals));
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return transform.apply(reader.visitNotIn(fieldRef, literals));
    }

    @Override
    public GlobalIndexResult visitTopK(TopK topK) {
        return transform.apply(reader.visitTopK(topK));
    }

    @Override
    public void close() throws IOException {
        closeable.close();
    }
}

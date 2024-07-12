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

package org.apache.paimon.fileindex.empty;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.predicate.FieldRef;

import java.util.List;

import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/** Empty file index which has no writer and no serialized bytes. */
public class EmptyFileIndexReader extends FileIndexReader {

    /** No data in the file index, which mean this file has no related records. */
    public static final EmptyFileIndexReader INSTANCE = new EmptyFileIndexReader();

    @Override
    public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    @Override
    public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return SKIP;
    }
}
